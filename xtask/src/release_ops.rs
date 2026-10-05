//! Thin network preparation, release planning and exact-payload acceptance.
use crate::delivery::{self, ACCEPTANCE, Acceptance, Artifact};
use std::{fs, path::Path, time::Duration};

/// Finds one required option value; duplicate options are rejected.
pub(crate) fn field(args: &[String], name: &str) -> Result<String, String> {
    let values = args
        .windows(2)
        .filter(|pair| pair[0] == name)
        .map(|pair| pair[1].clone())
        .collect::<Vec<_>>();
    if values.len() != 1 || values[0].starts_with("--") {
        return Err(format!("{name} requires one value"));
    }
    Ok(values[0].clone())
}

/// Parses a stable numeric X.Y.Z without path characters or prerelease ambiguity.
pub(crate) fn version(value: &str) -> Result<Vec<u64>, String> {
    let parts = value.split('.').collect::<Vec<_>>();
    if parts.len() != 3
        || parts.iter().any(|s| {
            s.is_empty()
                || s.bytes().any(|b| !b.is_ascii_digit())
                || (s.len() > 1 && s.starts_with('0'))
        })
    {
        return Err("version must be canonical X.Y.Z".into());
    }
    parts
        .iter()
        .map(|s| {
            s.parse::<u64>()
                .map_err(|_| "version exceeds numeric limit".into())
        })
        .collect()
}

/// Runs one bounded trusted command, rejecting a nonzero observed exit.
fn checked(root: &Path, program: &str, args: &[&str], seconds: u64) -> Result<(), String> {
    let argv = args.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>();
    if delivery::run(program, &argv, root, Duration::from_secs(seconds))?.0 != 0 {
        return Err(format!("{program} failed"));
    }
    Ok(())
}

/// Fetches the committed Cargo graph and explicitly refreshes cargo-deny's DB.
/// A missing/wrong tool or failed DB refresh is an error, never an offline PASS.
pub fn prepare(root: &Path) -> Result<(), String> {
    checked(root, "cargo", &["fetch", "--locked"], 600)?;
    let (code, out, _) = delivery::run(
        "cargo",
        &["deny".into(), "--version".into()],
        root,
        Duration::from_secs(15),
    )?;
    if code != 0 || String::from_utf8_lossy(&out).trim() != "cargo-deny 0.20.2" {
        return Err("cargo-deny 0.20.2 is required".into());
    }
    checked(root, "cargo", &["deny", "fetch"], 180)
}

/// Replaces only the workspace version line, preserving Cargo key order/comments.
fn cargo_version(text: &str, next: &str) -> Result<(String, String), String> {
    let parsed: toml::Value = toml::from_str(text).map_err(|_| "invalid Cargo.toml")?;
    let previous = parsed
        .get("workspace")
        .and_then(|value| value.get("package"))
        .and_then(|value| value.get("version"))
        .and_then(toml::Value::as_str)
        .ok_or("workspace version missing")?
        .to_owned();
    let mut section = false;
    let mut count = 0;
    let updated = text
        .split_inclusive('\n')
        .map(|line| {
            let trimmed = line.trim();
            if trimmed.starts_with('[') {
                section = trimmed == "[workspace.package]";
            }
            if section
                && trimmed.starts_with("version")
                && trimmed
                    .split('=')
                    .next()
                    .is_some_and(|key| key.trim() == "version")
            {
                count += 1;
                let quoted = format!("\"{previous}\"");
                line.replacen(&quoted, &format!("\"{next}\""), 1)
            } else {
                line.to_owned()
            }
        })
        .collect::<String>();
    if count != 1 {
        return Err("workspace version must have one standalone declaration".into());
    }
    let verify: toml::Value = toml::from_str(&updated).map_err(|_| "updated Cargo invalid")?;
    if verify["workspace"]["package"]["version"].as_str() != Some(next) {
        return Err("workspace version edit failed".into());
    }
    Ok((previous, updated))
}

/// Changes only the source descriptor's product version from the explicit plan.
/// Existing SDK, protocol and registration fields survive; no compiled registry
/// or CARGO_PKG_VERSION from the running old preparation tool is consulted.
fn registration_version(bytes: &[u8], next: &str) -> Result<Vec<u8>, String> {
    let mut descriptor: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|_| "source registration invalid")?;
    let field = descriptor
        .get_mut("product_version")
        .filter(|field| field.is_string())
        .ok_or("source registration product_version missing")?;
    *field = serde_json::json!(next);
    let mut bytes = serde_json::to_vec_pretty(&descriptor)
        .map_err(|_| "source registration serialization failed")?;
    bytes.push(b'\n');
    Ok(bytes)
}

/// Plans source version mirrors by default; --apply changes Cargo, CHANGELOG,
/// workspace lock entries and registration product_version without exporting an
/// old compiled registry. Failed lock update preserves the reviewable diff.
pub fn prepare_release(root: &Path, args: &[String]) -> Result<(), String> {
    let next = args.get(1).ok_or("release prepare VERSION [--apply]")?;
    let next_parts = version(next)?;
    if args.len() > 3 || args.iter().skip(2).any(|s| s != "--apply") {
        return Err("unknown release prepare option".into());
    }
    if !delivery::git(root, &["status", "--porcelain", "--untracked-files=normal"])?.is_empty() {
        return Err("release preparation requires clean source".into());
    }
    let cargo = fs::read_to_string(root.join("Cargo.toml")).map_err(|_| "Cargo unavailable")?;
    let (previous, updated) = cargo_version(&cargo, next)?;
    if next_parts <= version(&previous)? {
        return Err("release version must increase".into());
    }
    let changelog =
        fs::read_to_string(root.join("CHANGELOG.md")).map_err(|_| "CHANGELOG unavailable")?;
    if changelog
        .lines()
        .any(|s| s == format!("## {next}") || s == format!("## [{next}]"))
    {
        return Err("release CHANGELOG section already exists".into());
    }
    let headings = ["## Unreleased", "## [Unreleased]"];
    let heading = changelog
        .lines()
        .find(|s| headings.contains(s))
        .ok_or("Unreleased heading required")?;
    let section = changelog
        .split_once(heading)
        .ok_or("Unreleased section missing")?
        .1;
    if section
        .split("\n## ")
        .next()
        .unwrap_or_default()
        .trim()
        .is_empty()
    {
        return Err("release notes must not be empty".into());
    }
    let planned = changelog.replacen(heading, &format!("{heading}\n\n## {next}"), 1);
    let registration = root.join("schemas/mcp-registration.json");
    let registration_bytes =
        registration_version(&delivery::read_regular(&registration, 65536)?, next)?;
    println!(
        "{}",
        serde_json::json!({"previous":previous,"version":next,"apply":args.iter().any(|s|s=="--apply"),"files":["Cargo.toml","Cargo.lock","CHANGELOG.md","schemas/mcp-registration.json"]})
    );
    if args.iter().any(|s| s == "--apply") {
        fs::write(root.join("Cargo.toml"), updated).map_err(|_| "Cargo write failed")?;
        fs::write(root.join("CHANGELOG.md"), planned).map_err(|_| "CHANGELOG write failed")?;
        fs::write(registration, registration_bytes)
            .map_err(|_| "source registration write failed")?;
        checked(root, "cargo", &["update", "--offline", "--workspace"], 180)?;
    }
    Ok(())
}

/// Verifies clean exact HEAD, version notes and main ancestry. Optional tag check
/// requires an immutable annotated tag; no remote refs are fetched or pushed.
pub fn verify_source(
    root: &Path,
    accepted: &str,
    tagged: bool,
) -> Result<delivery::Identity, String> {
    let id = delivery::identity(root, accepted)?;
    version(&id.version)?;
    let main_ref = if std::env::var("GITHUB_ACTIONS").as_deref() == Ok("true")
        || delivery::git(root, &["rev-parse", "--verify", "refs/remotes/origin/main"]).is_ok()
    {
        "refs/remotes/origin/main"
    } else {
        "refs/heads/main"
    };
    checked(
        root,
        "git",
        &["merge-base", "--is-ancestor", accepted, main_ref],
        15,
    )?;
    let notes =
        fs::read_to_string(root.join("CHANGELOG.md")).map_err(|_| "CHANGELOG unavailable")?;
    let marker = format!("## {}", id.version);
    let bracketed = format!("## [{}]", id.version);
    let heading = notes
        .lines()
        .find(|line| *line == marker || *line == bracketed)
        .ok_or("version CHANGELOG section missing")?;
    if notes
        .split_once(heading)
        .ok_or("release notes missing")?
        .1
        .split("\n## ")
        .next()
        .unwrap_or_default()
        .trim()
        .is_empty()
    {
        return Err("release notes are empty".into());
    }
    if tagged
        && (delivery::git(root, &["cat-file", "-t", &id.tag])? != "tag"
            || delivery::git(root, &["rev-parse", &format!("{}^{{commit}}", id.tag)])? != accepted)
    {
        return Err("annotated exact-source tag required".into());
    }
    Ok(id)
}

/// Explicitly creates an annotated local tag at the accepted SHA after actual
/// successful local source/dependency evidence. Never pushes or overwrites a tag.
pub fn tag(root: &Path, args: &[String]) -> Result<(), String> {
    if args.len() != 6
        || !args[2..]
            .chunks(2)
            .all(|pair| ["--accepted-commit", "--evidence"].contains(&pair[0].as_str()))
    {
        return Err(
            "usage: release tag VERSION --accepted-commit FULL_SHA --evidence FILE; no push flag"
                .into(),
        );
    }
    let requested = args
        .get(1)
        .ok_or("release tag VERSION --accepted-commit FULL_SHA --evidence FILE")?;
    version(requested)?;
    let accepted = field(args, "--accepted-commit")?;
    let id = verify_source(root, &accepted, false)?;
    if &id.version != requested {
        return Err("tag/Cargo version mismatch".into());
    }
    let evidence: Acceptance = serde_json::from_slice(&delivery::read_regular(
        Path::new(&field(args, "--evidence")?),
        262144,
    )?)
    .map_err(|_| "tag evidence invalid")?;
    if evidence.identity != id
        || evidence.checks.iter().any(|c| c.exit_code != 0)
        || !["workspace", "dependency-policy", "integration"]
            .iter()
            .all(|name| {
                evidence
                    .checks
                    .iter()
                    .any(|c| c.check_id == *name && c.exit_code == 0)
            })
    {
        return Err("successful exact-source gate evidence required".into());
    }
    checked(
        root,
        "git",
        &[
            "tag",
            "-a",
            &id.tag,
            &accepted,
            "-m",
            &format!("Release {}", id.tag),
        ],
        15,
    )
}

/// Merges separately observed source/build jobs only when all source/run fields
/// match. The create-new output cannot silently replace another attempt's proof.
pub fn merge_evidence(root: &Path, args: &[String], accepted: &str) -> Result<(), String> {
    let id = delivery::identity(root, accepted)?;
    let mut merged: Option<Acceptance> = None;
    for option in ["--gate", "--native"] {
        let value: Acceptance = serde_json::from_slice(&delivery::read_regular(
            Path::new(&field(args, option)?),
            262144,
        )?)
        .map_err(|_| "producer evidence invalid")?;
        if value.schema_version != 1
            || value.identity != id
            || value.scope != "github-actions"
            || !value.qualified_hosts.is_empty()
            || value.checks.iter().any(|c| c.exit_code != 0)
        {
            return Err("producer evidence identity/outcome conflict".into());
        }
        if let Some(output) = &mut merged {
            output.checks.extend(value.checks);
            output.payload = value.payload;
        } else {
            merged = Some(value);
        }
    }
    delivery::create_json(
        &Path::new(&field(args, "--directory")?).join(ACCEPTANCE),
        &merged.ok_or("evidence absent")?,
    )
}

/// Thin local archive alias over bounded pre-extraction checks and the legacy
/// packaging seal. It neither verifies producer provenance nor executes payloads.
pub fn verify_archive(root: &Path, archive: &Path) -> Result<(), String> {
    crate::tar_guard::inspect(archive, true)?;
    let archive = fs::canonicalize(archive).map_err(|_| "archive unavailable")?;
    let temp = tempfile::tempdir().map_err(|_| "archive verification scratch unavailable")?;
    checked(
        root,
        "tar",
        &[
            "-xzf",
            archive.to_str().ok_or("archive path UTF-8 required")?,
            "-C",
            temp.path().to_str().ok_or("scratch path UTF-8 required")?,
            "--no-same-owner",
            "--no-same-permissions",
        ],
        120,
    )?;
    crate::release::verify(temp.path())
}

/// Installs the exact already-built archive twice into disposable paths, observes
/// its --version and records the archive bytes. It never declares host certification.
pub fn smoke(root: &Path, directory: &Path, accepted: &str) -> Result<(), String> {
    let id = delivery::identity(root, accepted)?;
    let mut evidence: Acceptance = serde_json::from_slice(&delivery::read_regular(
        &directory.join(ACCEPTANCE),
        262144,
    )?)
    .map_err(|_| "invalid acceptance")?;
    if evidence.identity != id || evidence.payload.is_some() {
        return Err("payload evidence identity/conflict".into());
    }
    let name = format!("agent-run-{}-aarch64-apple-darwin.tar.gz", id.version);
    let archive = directory.join(&name);
    crate::tar_guard::inspect(&archive, true)?;
    let temp = tempfile::tempdir().map_err(|_| "smoke scratch unavailable")?;
    let unpacked = temp.path().join("payload");
    fs::create_dir(&unpacked).map_err(|_| "smoke unpack unavailable")?;
    checked(
        root,
        "tar",
        &[
            "-xzf",
            archive.to_str().ok_or("archive path UTF-8 required")?,
            "-C",
            unpacked.to_str().ok_or("scratch path UTF-8 required")?,
            "--no-same-owner",
            "--no-same-permissions",
        ],
        120,
    )?;
    crate::release::verify(&unpacked)?;
    let helper = unpacked.join("bin/agent-run-deploy");
    let prefix = temp.path().join("prefix");
    let home = temp.path().join("home");
    let bins = temp.path().join("bin");
    let argv = vec![
        "install".into(),
        "--release".into(),
        unpacked.to_string_lossy().into(),
        "--prefix".into(),
        prefix.to_string_lossy().into(),
        "--home".into(),
        home.to_string_lossy().into(),
        "--bin-dir".into(),
        bins.to_string_lossy().into(),
        "--version".into(),
        id.version.clone(),
    ];
    let mut observations = Vec::new();
    let mut commands = Vec::new();
    let mut helper_argv = vec![helper.to_string_lossy().into_owned()];
    helper_argv.extend(argv.clone());
    for _ in 0..2 {
        let (code, out, err) = delivery::run(
            helper.to_str().ok_or("helper path invalid")?,
            &argv,
            root,
            Duration::from_secs(180),
        )?;
        if code != 0 {
            return Err("exact payload disposable install/no-op failed".into());
        }
        observations.push(delivery::check_record(&helper_argv, code, &out, &err)?);
        commands.push(helper_argv.clone());
    }
    let (code, out, err) = delivery::run(
        unpacked
            .join("bin/agent-run")
            .to_str()
            .ok_or("binary path invalid")?,
        &["--version".into()],
        root,
        Duration::from_secs(15),
    )?;
    if code != 0 || String::from_utf8_lossy(&out).trim() != format!("agent-run {}", id.version) {
        return Err("exact payload executable/version smoke failed".into());
    }
    let version_argv = vec![
        unpacked
            .join("bin/agent-run")
            .to_string_lossy()
            .into_owned(),
        "--version".to_owned(),
    ];
    observations.push(delivery::check_record(&version_argv, code, &out, &err)?);
    commands.push(version_argv);
    let mut check = delivery::check_record(
        &helper_argv,
        code,
        &serde_json::to_vec(&observations).map_err(|_| "smoke proof serialization failed")?,
        &err,
    )?;
    check.check_id = "exact-payload".into();
    check.argv_sha256 = crate::release::digest_bytes(
        &serde_json::to_vec(&commands).map_err(|_| "smoke argv serialization failed")?,
    );
    evidence.checks.push(check);
    evidence.payload = Some(Artifact {
        name,
        kind: "bundle".into(),
        target: Some(id.target),
        size: fs::metadata(&archive).map_err(|_| "archive missing")?.len(),
        sha256: crate::release::digest(&archive).map_err(|_| "archive hash failed")?,
    });
    let pending = directory.join("acceptance.pending.json");
    delivery::create_json(&pending, &evidence)?;
    fs::rename(pending, directory.join(ACCEPTANCE)).map_err(|_| "smoke evidence update failed")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    /// Cargo planning preserves neighboring inline tables, comments and key order.
    #[test]
    fn version_edit_is_narrow_and_versions_are_strict() {
        let input = "[workspace.package]\nlicense = \"MIT\"\nversion = \"0.19.4\" # keep\nfoo = { x = 1 }\n";
        let (_, output) = cargo_version(input, "0.20.0").unwrap();
        assert_eq!(output, input.replace("0.19.4", "0.20.0"));
        assert!(cargo_version("[workspace]\n", "0.20.0").is_err());
        for bad in ["v1.2.3", "1.2", "01.2.3", "1.2.3-beta", "1.2.3/"] {
            assert!(version(bad).is_err());
        }
    }
    /// Duplicate option values cannot silently select a different accepted source.
    #[test]
    fn duplicate_identity_option_is_refused() {
        assert!(
            field(
                &["--commit".into(), "a".into(), "--commit".into(), "b".into()],
                "--commit"
            )
            .is_err()
        );
    }
}
