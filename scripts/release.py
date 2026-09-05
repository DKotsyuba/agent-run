#!/usr/bin/env python3
"""Publish a version through GitHub's existing gates, then deploy verified assets.

Run from any directory with Python 3.14+. All subprocess inputs are argv lists;
GitHub, git, and the local deployment journal provide resumable state.
"""

from __future__ import annotations

import argparse
import datetime
import getpass
import hashlib
import json
import math
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time
import tomllib
from typing import Any

# Public repository and immutable workflow identity accepted for distribution.
REPOSITORY = "DKotsyuba/agent-run"
# Checkout containing this maintainer script; never inferred from the caller's cwd.
ROOT = Path(__file__).resolve().parents[1]


class ReleaseError(RuntimeError):
    """A failed release gate; repeating the command resumes recorded remote state."""


class Runner:
    """Own subprocess execution and a single monotonic timeout for the command."""

    def __init__(self, timeout: float, poll: float) -> None:
        """Set positive finite float timeout/poll seconds; return no value."""
        self.deadline = time.monotonic() + timeout
        self.poll = poll

    def run(self, *args: str, cwd: Path = ROOT, check: bool = True,
            env: dict[str, str] | None = None, input: str | None = None) -> subprocess.CompletedProcess[str]:
        """Run string argv in Path cwd with optional environment/input; raise on failure.

        Return captured text CompletedProcess. Each command is bounded by the
        remaining overall deadline and 120 seconds; check=False preserves errors.
        """
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise ReleaseError("Release deadline exceeded; rerun to resume")
        result = subprocess.run(args, cwd=cwd, env=env, input=input, text=True,
                                capture_output=True, timeout=min(120, remaining))
        if check and result.returncode:
            raise ReleaseError(f"{args[0]} {args[1] if len(args) > 1 else ''} failed: {result.stderr.strip() or result.stdout.strip()}")
        return result

    def json(self, *args: str) -> Any:
        """Run string argv and decode its stdout JSON; propagate command/JSON errors."""
        return json.loads(self.run(*args).stdout)

    def pause(self, stage: str) -> None:
        """Print string stage and wait poll seconds, raising at the global deadline."""
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise ReleaseError(f"Timed out waiting for {stage}; rerun to resume")
        print(f"Waiting: {stage}", flush=True)
        time.sleep(min(self.poll, remaining))


def optional_api(runner: Runner, endpoint: str) -> Any:
    """Return parsed GitHub endpoint JSON or None for HTTP 404 only; raise otherwise."""
    result = runner.run("gh", "api", endpoint, check=False)
    if result.returncode:
        if "HTTP 404" in result.stderr:
            return None
        raise ReleaseError(result.stderr.strip())
    return json.loads(result.stdout)


def tag_commit(runner: Runner, tag: str) -> str | None:
    """Resolve annotated remote tag string to commit SHA, rejecting lightweight tags."""
    ref = optional_api(runner, f"repos/{REPOSITORY}/git/ref/tags/{tag}")
    if ref is None:
        return None
    obj = ref["object"]
    if obj["type"] != "tag":
        raise ReleaseError(f"{tag} is not an annotated tag")
    tagged = runner.json("gh", "api", f"repos/{REPOSITORY}/git/tags/{obj['sha']}")
    if tagged["object"]["type"] != "commit":
        raise ReleaseError(f"{tag} must point directly to a commit")
    sha = tagged["object"]["sha"]
    content = runner.json("gh", "api", f"repos/{REPOSITORY}/contents/pyproject.toml?ref={sha}")
    import base64
    version = tomllib.loads(base64.b64decode(content["content"]).decode())["project"]["version"]
    if tag != f"v{version}":
        raise ReleaseError(f"Immutable tag {tag} does not match package version {version}")
    return sha


def wait_workflow(runner: Runner, workflow: str, sha: str, event: str) -> None:
    """Wait for nonempty completed successful workflow at exact SHA/event strings.

    Missing/pending runs wait; a completed unsuccessful latest run fails. Return
    None only after checking both the run and every reported job.
    """
    while True:
        runs = runner.json("gh", "run", "list", "--repo", REPOSITORY, "--workflow", workflow,
                           "--commit", sha, "--event", event, "--limit", "100",
                           "--json", "databaseId,headSha,status,conclusion,event")
        exact = [r for r in runs if r["headSha"] == sha and r["event"] == event]
        if exact:
            run = exact[0]
            if run["status"] == "completed":
                if run["conclusion"] != "success":
                    raise ReleaseError(f"{workflow} at {sha} ended {run['conclusion']}")
                detail = runner.json("gh", "run", "view", str(run["databaseId"]), "--repo", REPOSITORY,
                                     "--json", "headSha,jobs")
                if detail["headSha"] != sha:
                    raise ReleaseError("Workflow head changed")
                jobs = detail["jobs"]
                if jobs and all(j["conclusion"] == "success" for j in jobs):
                    return
                if any(j.get("conclusion") not in (None, "", "success") for j in jobs):
                    raise ReleaseError(f"{workflow} has unsuccessful jobs")
        runner.pause(f"{workflow} at {sha}")


def wait_required(runner: Runner, number: int, sha: str) -> None:
    """Require nonempty passing required PR checks and unchanged exact head SHA."""
    while True:
        head = runner.json("gh", "pr", "view", str(number), "--repo", REPOSITORY, "--json", "headRefOid")
        if head["headRefOid"] != sha:
            raise ReleaseError("Pull request head changed; inspect it before resuming")
        checks = runner.run("gh", "pr", "checks", str(number), "--repo", REPOSITORY,
                            "--required", "--json", "name,bucket", check=False)
        if checks.returncode not in (0, 1, 8):
            raise ReleaseError(checks.stderr.strip())
        rows = json.loads(checks.stdout) if checks.stdout.strip() else []
        if any(c["bucket"] in ("fail", "cancel", "skipping") for c in rows):
            raise ReleaseError("Required pull request check failed/cancelled/skipped")
        if rows and all(c["bucket"] == "pass" for c in rows):
            return
        runner.pause("required pull request checks")


def prepare_pr(runner: Runner, version: str) -> dict:
    """Reuse/create deterministic version-only branch and PR; return PR metadata.

    Require clean checkout on origin/main before new publication. Persist a
    dedicated worktree under .git for interrupted commits/pushes; never stage
    anything except pyproject.toml and CHANGELOG.md.
    """
    if runner.run("git", "status", "--porcelain", "--untracked-files=all").stdout:
        raise ReleaseError("New publication requires a clean committed checkout")
    runner.run("git", "fetch", "origin", "main")
    branch = f"release/v{version}"
    prs = runner.json("gh", "pr", "list", "--repo", REPOSITORY, "--head", branch, "--state", "all",
                      "--json", "number,state,headRefOid,mergeCommit,baseRefName")
    if prs:
        if len(prs) != 1 or prs[0]["baseRefName"] != "main" or prs[0]["state"] == "CLOSED":
            raise ReleaseError("Ambiguous or closed release pull request")
        return prs[0]
    head = runner.run("git", "rev-parse", "HEAD").stdout.strip()
    main = runner.run("git", "rev-parse", "origin/main").stdout.strip()
    if head != main:
        raise ReleaseError("Commit source changes and update checkout to origin/main first")
    gitdir = Path(runner.run("git", "rev-parse", "--absolute-git-dir").stdout.strip())
    work = gitdir / "release-worktrees" / version
    if not work.exists():
        remote = runner.run("git", "ls-remote", "--heads", "origin", branch).stdout.strip()
        if remote:
            runner.run("git", "fetch", "origin", f"{branch}:{branch}")
        exists = runner.run("git", "show-ref", "--verify", f"refs/heads/{branch}", check=False).returncode == 0
        work.parent.mkdir(parents=True, exist_ok=True)
        argv = ["git", "worktree", "add", str(work)]
        runner.run(*(argv + [branch] if exists else argv + ["-b", branch, main]))
    package = work / "pyproject.toml"
    old = package.read_text()
    current = tomllib.loads(old)["project"]["version"]
    if current != version:
        if tuple(map(int, version.split("."))) <= tuple(map(int, current.split("."))):
            raise ReleaseError("New version must increase the package version")
        package.write_text(re.sub(r'^version = "[^"]+"$', f'version = "{version}"', old, count=1, flags=re.M))
    changelog = work / "CHANGELOG.md"
    history = changelog.read_text()
    if f"## [{version}]" not in history:
        previous = runner.run("git", "describe", "--tags", "--abbrev=0", cwd=work, check=False)
        span = f"{previous.stdout.strip()}..HEAD" if previous.returncode == 0 else "HEAD"
        subjects = runner.run("git", "log", "--format=%s", "--no-merges", span, cwd=work).stdout.splitlines()
        notes = "\n".join(f"- {subject}" for subject in subjects) or "- Maintenance release."
        insertion = f"## [{version}] - {datetime.date.today().isoformat()}\n\n{notes}\n\n"
        index = history.find("## [")
        changelog.write_text(history[:index] + insertion + history[index:] if index >= 0 else history + "\n" + insertion)
    if runner.run("git", "status", "--porcelain", cwd=work).stdout:
        runner.run("git", "add", "--", "pyproject.toml", "CHANGELOG.md", cwd=work)
        staged = runner.run("git", "diff", "--cached", "--name-only", cwd=work).stdout.splitlines()
        if set(staged) - {"pyproject.toml", "CHANGELOG.md"}:
            raise ReleaseError("Unexpected staged files in release worktree")
        runner.run("git", "commit", "-m", f"chore: release {version}", cwd=work)
    runner.run("git", "push", "origin", branch, cwd=work)
    runner.run("gh", "pr", "create", "--repo", REPOSITORY, "--base", "main", "--head", branch,
               "--title", f"Release {version}", "--body", f"Update package version and changelog for {version}. CI verifies the release source.")
    return runner.json("gh", "pr", "view", branch, "--repo", REPOSITORY,
                       "--json", "number,state,headRefOid,mergeCommit,baseRefName")


def publish(runner: Runner, version: str) -> str:
    """Return immutable published version commit SHA, resuming remote gates safely."""
    tag = f"v{version}"
    release = optional_api(runner, f"repos/{REPOSITORY}/releases/tags/{tag}")
    sha = tag_commit(runner, tag)
    if release and not release["draft"]:
        if not sha or release["tag_name"] != tag or release.get("prerelease"):
            raise ReleaseError("Published release has inconsistent immutable tag")
        print(f"Published {tag} already exists at {sha}; verifying assets", flush=True)
        return sha
    if not sha:
        pr = prepare_pr(runner, version)
        if pr["state"] != "MERGED":
            wait_workflow(runner, "ci.yml", pr["headRefOid"], "pull_request")
            wait_required(runner, pr["number"], pr["headRefOid"])
            runner.run("gh", "pr", "merge", str(pr["number"]), "--repo", REPOSITORY,
                       "--squash", "--match-head-commit", pr["headRefOid"])
        while True:
            pr = runner.json("gh", "pr", "view", str(pr["number"]), "--repo", REPOSITORY,
                             "--json", "state,mergeCommit")
            if pr["state"] == "MERGED" and pr["mergeCommit"]:
                break
            runner.pause("pull request merge")
        sha = pr["mergeCommit"]["oid"]
        wait_workflow(runner, "ci.yml", sha, "push")
        runner.run("git", "fetch", "origin", "main")
        existing = tag_commit(runner, tag)
        if existing and existing != sha:
            raise ReleaseError("Remote tag already points to a different commit")
        if not existing:
            local = runner.run("git", "rev-parse", "--verify", f"refs/tags/{tag}^{{commit}}", check=False)
            if local.returncode == 0:
                if local.stdout.strip() != sha or runner.run("git", "cat-file", "-t", tag).stdout.strip() != "tag":
                    raise ReleaseError("Local tag conflicts with accepted commit")
            else:
                runner.run("git", "tag", "-a", tag, sha, "-m", f"agent-run {version}")
            runner.run("git", "push", "origin", f"refs/tags/{tag}")
    wait_workflow(runner, "release.yml", sha, "push")
    while True:
        release = optional_api(runner, f"repos/{REPOSITORY}/releases/tags/{tag}")
        if release and not release["draft"]:
            if tag_commit(runner, tag) != sha:
                raise ReleaseError("Release tag changed during publication")
            return sha
        runner.pause("public GitHub Release")


def verify_assets(runner: Runner, version: str, sha: str, directory: Path) -> Path:
    """Download distributions into Path directory and verify hashes/provenance.

    Return verified wheel Path. Reject malformed checksums or an attestation
    whose signed subject, source commit/ref, repository or workflow differ.
    """
    names = [f"agent_run-{version}-py3-none-any.whl", f"agent_run-{version}.tar.gz"]
    runner.run("gh", "release", "download", f"v{version}", "--repo", REPOSITORY,
               "--dir", str(directory), "--pattern", names[0], "--pattern", names[1], "--pattern", "SHA256SUMS")
    expected = {}
    for line in (directory / "SHA256SUMS").read_text().splitlines():
        match = re.fullmatch(r"([0-9a-f]{64})  (.+)", line)
        if not match or match[2] not in names or match[2] in expected:
            raise ReleaseError("Malformed or unexpected release checksum entry")
        expected[match[2]] = match[1]
    if set(expected) != set(names):
        raise ReleaseError("Missing distribution checksums")
    for name in names:
        path = directory / name
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest != expected[name]:
            raise ReleaseError(f"Checksum mismatch: {name}")
        results = runner.json("gh", "attestation", "verify", str(path), "--repo", REPOSITORY,
                              "--signer-workflow", f"{REPOSITORY}/.github/workflows/release.yml", "--format", "json")
        valid = False
        for item in results:
            result = item.get("verificationResult", {})
            cert = result.get("signature", {}).get("certificate", {})
            subject = result.get("statement", {}).get("subject", [])
            valid |= (cert.get("sourceRepositoryDigest") == sha
                      and cert.get("sourceRepositoryRef") == f"refs/tags/v{version}"
                      and cert.get("buildConfigURI") == f"https://github.com/{REPOSITORY}/.github/workflows/release.yml@refs/tags/v{version}"
                      and any(s.get("name") == name and s.get("digest", {}).get("sha256") == digest for s in subject))
        if not valid:
            raise ReleaseError(f"Provenance identity mismatch: {name}")
    return directory / names[0]


def main(argv: list[str] | None = None) -> int:
    """Parse optional string argv; publish/deploy and return process status 0 or 1."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("version")
    parser.add_argument("--publish-only", action="store_true")
    parser.add_argument("--home", type=Path, default=Path.home() / ".agent-run")
    parser.add_argument("--python", default="python3.14")
    parser.add_argument("--launchd-prefix", default=f"com.{getpass.getuser()}.agent-run")
    parser.add_argument("--timeout", type=float, default=3600)
    parser.add_argument("--poll", type=float, default=10)
    args = parser.parse_args(argv)
    if not re.fullmatch(r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)", args.version):
        parser.error("version must be strict X.Y.Z")
    if any(not math.isfinite(n) or n <= 0 for n in (args.timeout, args.poll)):
        parser.error("timeout and poll must be positive finite seconds")
    try:
        runner = Runner(args.timeout, args.poll)
        sha = publish(runner, args.version)
        with tempfile.TemporaryDirectory(prefix="agent-run-release-") as temporary:
            wheel = verify_assets(runner, args.version, sha, Path(temporary))
            if not args.publish_only:
                from release_local import deploy
                deploy(runner, args.home.expanduser().resolve(), wheel, args.version, sha,
                       args.python, args.launchd_prefix)
        print(f"Done: v{args.version} published" + (" and local runtime updated. Reconnect existing MCP clients." if not args.publish_only else "."))
        return 0
    except (ReleaseError, OSError, ValueError, subprocess.SubprocessError) as error:
        print(f"Release stopped: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    # Keep the exception/runner identity shared with the local deployment module.
    sys.modules["release"] = sys.modules[__name__]
    raise SystemExit(main())
