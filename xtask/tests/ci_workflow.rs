//! Regression coverage for the checked-in CI workflow files.
//!
//! Mirrors `tests/test_ci.py`. These assertions are entirely about static
//! `.github/workflows/*.yml` text and a literal bash block; the Rust
//! migration has not touched those Python-build CI steps yet (a parallel
//! `.github/workflows/rust.yml` validates the Rust workspace separately),
//! so the ported checks run unmodified against the same files and same
//! literal substrings the Python test reads.

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

/// Resolves a workflow file relative to this crate's manifest, matching
/// Python's `Path(__file__).parents[1] / ".github/workflows"`.
fn workflow_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join(".github/workflows")
        .join(name)
}

/// Extracts and dedents the "Run tests" step's `run: |` body, mirroring
/// `CiRetryTests._run_tests_block`.
fn run_tests_block(workflow: &str) -> String {
    let lines: Vec<&str> = workflow.lines().collect();
    let start = lines
        .iter()
        .position(|line| *line == "      - name: Run tests")
        .expect("Run tests step missing from CI workflow");
    let run_line = lines[start..]
        .iter()
        .position(|line| *line == "        run: |")
        .map(|offset| start + offset)
        .expect("Run tests step has no run block");
    let mut block = String::new();
    for line in &lines[run_line + 1..] {
        if line.starts_with("      - name: ") {
            break;
        }
        match line.strip_prefix("          ") {
            Some(dedented) => block.push_str(dedented),
            None => block.push_str(line),
        }
        block.push('\n');
    }
    block
}

fn must_find(haystack: &str, needle: &str) -> usize {
    haystack
        .find(needle)
        .unwrap_or_else(|| panic!("workflow is missing {needle:?}"))
}

fn must_find_from(haystack: &str, needle: &str, from: usize) -> usize {
    from + haystack[from..]
        .find(needle)
        .unwrap_or_else(|| panic!("workflow is missing {needle:?} after byte {from}"))
}

/// Mirrors `tests/test_ci.py::CiRetryTests::test_run_tests_block_preserves_first_failure_and_runs_diagnostics`.
#[test]
fn python_ci_run_tests_block_preserves_first_failure_and_runs_diagnostics() {
    let workflow = fs::read_to_string(workflow_path("ci.yml")).expect("read ci.yml");
    let block = run_tests_block(&workflow);

    let directory = tempfile::tempdir().expect("scratch directory");
    let fake_bin = directory.path().join("bin");
    fs::create_dir(&fake_bin).expect("fake bin directory");
    let count = directory.path().join("count");
    let fake_python = fake_bin.join("python");
    fs::write(
        &fake_python,
        "#!/bin/sh\n\
         count=\"$FAKE_COUNT_FILE\"\n\
         calls=$(cat \"$count\" 2>/dev/null || echo 0)\n\
         calls=$((calls + 1)); echo \"$calls\" > \"$count\"\n\
         if [ \"$calls\" -eq 1 ] && { [ \"$FAKE_MODE\" = firstfail ] || [ \"$FAKE_MODE\" = diagfail ]; }; then exit 7; fi\n\
         if [ \"$FAKE_MODE\" = diagfail ] && [ \"$calls\" -eq 2 ]; then exit 9; fi\n\
         exit 0\n",
    )
    .expect("write fake python");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&fake_python, fs::Permissions::from_mode(0o755))
            .expect("chmod fake python");
    }

    let path = format!(
        "{}:{}",
        fake_bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    for (mode, expected_status, expected_calls) in
        [("allpass", 0, 1), ("firstfail", 7, 3), ("diagfail", 7, 3)]
    {
        let _ = fs::remove_file(&count);
        let output = Command::new("bash")
            .args([
                "--noprofile",
                "--norc",
                "-e",
                "-o",
                "pipefail",
                "-c",
                &block,
            ])
            .env("PATH", &path)
            .env("RUNNER_OS", "Linux")
            .env("FAKE_COUNT_FILE", &count)
            .env("FAKE_MODE", mode)
            .output()
            .expect("run fake CI block");
        assert_eq!(
            output.status.code(),
            Some(expected_status),
            "mode {mode}: stderr={}",
            String::from_utf8_lossy(&output.stderr)
        );
        let calls: u32 = fs::read_to_string(&count)
            .expect("call count file")
            .trim()
            .parse()
            .expect("call count is numeric");
        assert_eq!(calls, expected_calls, "mode {mode}");
    }
}

/// Mirrors `tests/test_ci.py::CiRetryTests::test_release_workflows_publish_verified_lock_before_smoke_and_checksums`.
#[test]
fn python_ci_release_workflows_publish_verified_lock_before_smoke_and_checksums() {
    let ci = fs::read_to_string(workflow_path("ci.yml")).expect("read ci.yml");
    let release = fs::read_to_string(workflow_path("release.yml")).expect("read release.yml");

    for workflow in [&ci, &release] {
        for needle in [
            "python -m pip install uv==0.11.1",
            "uv lock --check",
            "--no-install-project --no-build",
            "--no-build-isolation --no-deps --editable .",
            "uv export --frozen --no-dev --no-editable --no-emit-project --format requirements-txt --output-file dist/requirements.lock",
            "-m pip install --require-hashes --only-binary=:all: -r dist/requirements.lock",
            "-m pip check",
            "python -m build --no-isolation",
            "--output-file dist/build-requirements.lock",
        ] {
            assert!(workflow.contains(needle), "workflow is missing {needle:?}");
        }
        assert!(
            must_find(workflow, "uv export --frozen") < must_find(workflow, "requirements.lock")
        );

        let wheel = must_find(workflow, "python -m venv \"$RUNNER_TEMP/wheel-smoke\"");
        let wheel_lock = must_find_from(workflow, "-r dist/requirements.lock", wheel);
        let wheel_artifact = must_find_from(workflow, "-m pip install --no-deps dist/*.whl", wheel);
        assert!(wheel_lock < wheel_artifact);
        assert!(wheel_artifact < must_find_from(workflow, "-m pip check", wheel));

        let sdist = must_find(workflow, "python -m venv \"$RUNNER_TEMP/sdist-smoke\"");
        let sdist_lock_install = must_find_from(
            workflow,
            "-m pip install --require-hashes --only-binary=:all: -r dist/build-requirements.lock",
            sdist,
        );
        assert!(
            sdist_lock_install
                < must_find_from(
                    workflow,
                    "--no-build-isolation --no-deps dist/*.tar.gz",
                    sdist
                )
        );

        let bootstrap = must_find(workflow, "uv sync --locked");
        let editable_install = must_find_from(
            workflow,
            "--no-build-isolation --no-deps --editable .",
            bootstrap,
        );
        assert!(bootstrap < editable_install);
        let after_editable_install = if workflow[bootstrap..].contains("python -m pytest") {
            must_find_from(workflow, "python -m pytest", bootstrap)
        } else {
            must_find_from(workflow, "python -m build", bootstrap)
        };
        assert!(editable_install < after_editable_install);
    }

    assert!(release.contains("requirements.lock > SHA256SUMS"));
    assert!(release.contains("dist/requirements.lock dist/SHA256SUMS"));
    assert!(release.contains("subject-checksums: dist/SHA256SUMS"));
}
