//! Static regression coverage for the native CI and release workflows.

use std::{fs, path::Path};

/// Reads a repository file relative to the workspace root.
fn repository_file(path: &str) -> String {
    fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join(path))
        .unwrap_or_else(|error| panic!("cannot read {path}: {error}"))
}

/// Ensures primary CI gates macOS releases while retaining visible Linux validation.
#[test]
fn ci_checks_native_release_and_desktop_transport() {
    let workflow = repository_file(".github/workflows/ci.yml");
    for required in [
        "cargo xtask check",
        "cargo xtask archive --verify",
        "cargo xtask release build-native",
        "cargo xtask release verify",
        "- os: macos-15\n            label: supported-macos-arm64\n            target: aarch64-apple-darwin\n            architecture: arm64",
        "- os: ubuntu-latest\n            label: validation-only-linux-x86_64-unqualified\n            target: x86_64-unknown-linux-gnu\n            architecture: x86_64",
        "continue-on-error: ${{ matrix.continue_on_error }}",
        "continue_on_error: false",
        "continue_on_error: true",
        "test \"$(uname -m)\" = \"${{ matrix.architecture }}\"",
        "node --test scripts/check-desktop-transport.cjs",
        "validation-only-linux-x86_64-unqualified",
    ] {
        assert!(workflow.contains(required), "CI is missing {required:?}");
    }
    assert!(
        !workflow.contains("macos-latest"),
        "CI must pin the arm64 runner"
    );
    let (matrix_job, release_contract) = workflow
        .split_once("\n  release-contract:")
        .expect("CI must retain its release-contract job");
    for (name, job) in [
        ("matrix", matrix_job),
        ("release-contract", release_contract),
    ] {
        let fetch = job
            .find("cargo fetch --locked")
            .unwrap_or_else(|| panic!("{name} job must fetch the locked graph"));
        let gate = job
            .find("cargo xtask")
            .unwrap_or_else(|| panic!("{name} job must run an xtask gate"));
        assert!(
            fetch < gate,
            "{name} job must fetch before offline xtask use"
        );
    }
}

/// One main-only producer owns each workload cache; releases/PRs only restore.
#[test]
fn rust_dependency_cache_policy_preserves_trust_and_runner_boundaries() {
    let ci = repository_file(".github/workflows/ci.yml");
    let release = repository_file(".github/workflows/release.yml");
    assert!(
        ci.contains("shared-key: agent-run-check-v2-${{ matrix.os }}-${{ matrix.architecture }}")
    );
    let package = ci
        .split("\n  package-cache:")
        .nth(1)
        .unwrap()
        .split("\n  dependencies:")
        .next()
        .unwrap();
    assert!(package.contains("if: github.event_name == 'push' && github.ref == 'refs/heads/main'"));
    assert!(package.contains("shared-key: agent-run-package-v1-macos-15-arm64"));
    assert!(package.contains("--target aarch64-apple-darwin"));
    let contract = ci
        .split("\n  release-contract:")
        .nth(1)
        .unwrap()
        .split("\n  package-cache:")
        .next()
        .unwrap();
    assert!(contract.contains("save-if: false"));
    assert!(contract.contains("shared-key: agent-run-package-v1-macos-15-arm64"));
    assert!(contract.contains("--target aarch64-apple-darwin"));
    assert_eq!(
        release
            .matches("shared-key: agent-run-check-v2-macos-15-arm64")
            .count(),
        1
    );
    assert_eq!(
        release
            .matches("shared-key: agent-run-package-v1-macos-15-arm64")
            .count(),
        2
    );
    assert_eq!(release.matches("save-if: false").count(), 3);
    assert!(!release.contains("actions/cache/save@"));
    assert!(!release.contains("save-if: ${{"));
    for workflow in [&ci, &release] {
        assert!(!workflow.contains("pull_request_target"));
        for line in workflow
            .lines()
            .filter(|line| line.trim().starts_with("uses:"))
        {
            let pin = line
                .split('@')
                .nth(1)
                .unwrap()
                .split_whitespace()
                .next()
                .unwrap();
            assert_eq!(pin.len(), 40, "action must have full SHA: {line}");
            assert!(pin.bytes().all(|b| b.is_ascii_hexdigit()), "{line}");
        }
        assert!(workflow.contains("cache-bin: false"));
        assert!(workflow.contains("cargo-deny 0.20.2"));
        assert!(workflow.contains("cargo-deny-"));
    }
    assert!(ci.contains("name: Dependency policy"));
    assert!(ci.contains("if: runner.os != 'macOS'"));
}

/// Publisher fans in independently checked source and one exact macOS payload;
/// cache hits never replace gates, draft verification or post-publication checks.
#[test]
fn release_publishes_checksummed_native_assets() {
    let workflow = repository_file(".github/workflows/release.yml");
    let (header, jobs) = workflow.split_once("\njobs:").unwrap();
    let (gate, rest) = jobs.split_once("\n  native:").unwrap();
    let (native, publish) = rest.split_once("\n  publish:").unwrap();
    assert!(header.contains("group: agent-run-release-publisher"));
    assert!(header.contains("cancel-in-progress: false"));
    assert!(
        header.contains("queue: max"),
        "default single pending run would cancel older queued tags"
    );
    assert!(header.contains("permissions:\n  contents: read"));
    assert!(!gate.contains("contents: write") && !native.contains("contents: write"));
    assert!(!native.contains("needs:"));
    assert!(!native.contains("cargo xtask check"));
    assert!(gate.contains("-- cargo xtask check"));
    assert!(gate.contains("-- cargo deny --offline --locked check"));
    assert_eq!(
        workflow
            .matches("-- cargo xtask release build-native")
            .count(),
        1
    );
    assert_eq!(
        workflow
            .matches("-- cargo xtask archive --revision HEAD")
            .count(),
        1
    );
    assert!(native.contains("cargo xtask package smoke"));
    assert!(native.contains("COPYFILE_DISABLE=1 tar --format=ustar"));
    assert!(publish.contains("needs: [gate, native]"));
    assert!(!publish.contains("release build-native"));
    for permission in [
        "contents: write",
        "id-token: write",
        "attestations: write",
        "artifact-metadata: write",
    ] {
        assert!(publish.contains(permission));
    }
    for required in [
        "env -u GH_TOKEN -u GITHUB_TOKEN",
        "cargo build --offline --locked --release -p xtask",
        "package merge-evidence",
        "package create",
        "package verify",
        "subject-checksums: dist/SHA256SUMS",
        "release publish --accepted-commit",
        "--run-id \"$GITHUB_RUN_ID\"",
        "--attempt \"$GITHUB_RUN_ATTEMPT\"",
    ] {
        assert!(publish.contains(required), "{required}");
    }
    assert!(
        publish.find("package verify").unwrap() < publish.find("Stage verify publish").unwrap()
    );
    assert!(!workflow.contains("x86_64-unknown-linux-gnu") && !workflow.contains("macos-latest"));
    let publisher = repository_file("xtask/src/release_publish.rs");
    for invariant in [
        "existing draft requires manual reconciliation",
        "--draft",
        "--draft=false",
        "remote(root, &manifest, &draft, false",
        "remote(root, &manifest, &published, true",
        "--signer-workflow",
        "--source-digest",
    ] {
        assert!(publisher.contains(invariant), "{invariant}");
    }
}

/// Ensures dependency automation covers both workflow actions and locked Rust crates weekly.
#[test]
fn dependabot_checks_actions_and_cargo_weekly() {
    let configuration = repository_file(".github/dependabot.yml");
    for required in [
        "package-ecosystem: github-actions",
        "package-ecosystem: cargo",
        "interval: weekly",
    ] {
        assert!(
            configuration.contains(required),
            "Dependabot is missing {required:?}"
        );
    }
}

/// Pins the xtask alias, local checks, and native release to the reviewed lockfile.
#[test]
fn xtask_entrypoints_and_builds_use_locked_resolution() {
    let alias = repository_file(".cargo/config.toml");
    assert!(
        alias.contains("xtask = \"run --locked --package xtask --\""),
        "the xtask alias must use the committed lockfile"
    );
    let source = repository_file("xtask/src/main.rs");
    assert!(
        source.contains("\"clippy\",\n            \"--offline\",\n            \"--locked\","),
        "cargo xtask check must lock clippy resolution"
    );
    assert!(
        source.contains("\"--\",\n            \"-D\",\n            \"warnings\","),
        "cargo xtask check must deny clippy warnings"
    );
    assert!(
        source.contains("\"test\",\n            \"--offline\",\n            \"--locked\","),
        "cargo xtask check must lock test resolution"
    );
    assert!(
        source.contains(
            "\"build\",\n        \"--offline\",\n        \"--locked\",\n        \"--release\","
        ),
        "native release builds must use the committed lockfile"
    );
}
