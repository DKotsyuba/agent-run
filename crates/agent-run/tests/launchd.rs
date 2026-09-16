//! LaunchAgent descriptor parity checks; these never invoke `launchctl`.

use agent_run::{launchd, Error};
use std::path::Path;
use tempfile::tempdir;

/// Mirrors `tests/test_capacity_launchd.py` configured one-shot capacity plist.
#[test]
fn python_capacity_launchd_has_one_shot_contract() {
    let temporary = tempdir().expect("temporary paths");
    let home = temporary.path().join("home");
    let binary = std::env::current_exe().expect("test executable");
    let rendered = launchd::render(
        &home,
        binary,
        "capacity",
        321,
        "com.example.capacity",
        temporary.path().join("capacity.out"),
        temporary.path().join("capacity.err"),
    );
    if cfg!(target_os = "macos") {
        let value = rendered.expect("macOS launchd descriptor");
        assert_eq!(value["interval_seconds"], 321);
        assert_eq!(value["argv"][1], "capacity");
        let plist = value["plist"].as_str().expect("plist text");
        assert!(plist.contains("<key>StartInterval</key><integer>321</integer>"));
        assert!(plist.contains("<key>RunAtLoad</key><false/>"));
        assert!(!plist.contains("<key>KeepAlive</key>"));
    } else {
        assert!(rendered.is_err(), "Linux must explicitly reject launchd");
    }
}

/// Mirrors `tests/test_cli.py` API and delivery launchd descriptor fields.
#[test]
fn python_api_and_delivery_launchd_keep_distinct_contracts() {
    let temporary = tempdir().expect("temporary paths");
    let home = temporary.path().join("home");
    let binary = std::env::current_exe().expect("test executable");
    for (kind, expected) in [
        ("api", "<key>SoftResourceLimits</key>"),
        ("delivery", "<key>StartInterval</key>"),
    ] {
        let result = launchd::render(
            &home,
            binary.clone(),
            kind,
            2,
            &format!("com.example.{kind}"),
            temporary.path().join(format!("{kind}.out")),
            temporary.path().join(format!("{kind}.err")),
        );
        if cfg!(target_os = "macos") {
            let plist = result.expect("macOS plist")["plist"]
                .as_str()
                .expect("plist text")
                .to_owned();
            assert!(plist.contains(expected));
            if kind == "api" {
                assert!(plist.contains("<key>KeepAlive</key><true/>"));
            }
            if kind == "delivery" {
                assert!(!plist.contains("<key>EnvironmentVariables</key>"));
            }
        } else {
            assert!(result.is_err());
        }
    }
}

/// Mirrors `tests/test_capacity_launchd.py::test_interval_must_be_a_positive_integer`.
///
/// Python's `build_job` also rejects `bool` and non-integer (e.g. `1.5`)
/// interval values; Rust's `interval: u64` parameter makes those inputs
/// impossible to construct at the call site, so only the shared "must be at
/// least 1" edge (`0`) is a live behavior to port here
/// (crates/agent-run/src/launchd.rs render, the `schedule` match).
#[test]
fn python_capacity_launchd_rejects_zero_interval() {
    let temporary = tempdir().expect("temporary paths");
    let home = temporary.path().join("home");
    let binary = std::env::current_exe().expect("test executable");
    let result = launchd::render(
        &home,
        binary,
        "capacity",
        0,
        "com.example.capacity",
        temporary.path().join("out.log"),
        temporary.path().join("err.log"),
    );
    if cfg!(target_os = "macos") {
        assert!(
            matches!(result, Err(Error::Validation(_))),
            "zero interval must be a validation error, got {result:?}"
        );
    } else {
        assert!(result.is_err(), "Linux must explicitly reject launchd");
    }
}

/// Mirrors `tests/test_capacity_launchd.py::test_configured_job_consumes_the_capacity_interval`.
///
/// Python routes `CapacityConfig.collect_interval_seconds` through a
/// dedicated `build_configured_job` helper; Rust's CLI arm
/// (crates/agent-run/src/cli.rs, `Capacity::Launchd`) reads the same config
/// field and passes it straight through as `render`'s `interval` argument, so
/// the behavior this test protects -- the configured interval reaching the
/// rendered job unchanged -- is exercised directly against `render`.
#[test]
fn python_capacity_launchd_configured_job_consumes_interval() {
    let temporary = tempdir().expect("temporary paths");
    let home = temporary.path().join("home");
    let binary = Path::new("/opt/agent&run").to_path_buf();
    let rendered = launchd::render(
        &home,
        binary,
        "capacity",
        17,
        "com.example.capacity",
        temporary.path().join("out.log"),
        temporary.path().join("err.log"),
    );
    if cfg!(target_os = "macos") {
        let value = rendered.expect("macOS launchd descriptor");
        assert_eq!(value["interval_seconds"], 17);
        assert_eq!(
            value["argv"],
            serde_json::json!(["/opt/agent&run", "capacity", "collect", "--once"])
        );
    } else {
        assert!(rendered.is_err(), "Linux must explicitly reject launchd");
    }
}

/// Mirrors `tests/test_capacity_launchd.py::test_plist_is_escaped_bounded_and_one_shot`.
#[test]
fn python_capacity_launchd_plist_is_escaped_bounded_and_one_shot() {
    let temporary = tempdir().expect("temporary paths");
    let home = temporary.path().join("home");
    let rendered = launchd::render(
        &home,
        Path::new("/opt/agent&run").to_path_buf(),
        "capacity",
        60,
        "com.example.<capacity&>",
        Path::new("/tmp/capacity<out>.log").to_path_buf(),
        Path::new("/tmp/capacity&err.log").to_path_buf(),
    );
    if cfg!(target_os = "macos") {
        let value = rendered.expect("macOS launchd descriptor");
        let plist = value["plist"].as_str().expect("plist text");
        assert!(plist.contains("com.example.&lt;capacity&amp;&gt;"));
        assert!(plist.contains("<key>StartInterval</key><integer>60</integer>"));
        assert!(plist.contains("<key>RunAtLoad</key><false/>"));
        assert!(!plist.contains("<key>KeepAlive</key>"));
        assert!(plist
            .contains("<key>StandardOutPath</key><string>/tmp/capacity&lt;out&gt;.log</string>"));
        assert!(plist
            .contains("<key>StandardErrorPath</key><string>/tmp/capacity&amp;err.log</string>"));
    } else {
        assert!(rendered.is_err(), "Linux must explicitly reject launchd");
    }
}

/// Mirrors `tests/test_capacity_launchd.py::test_plist_copies_only_invoking_home_and_path`.
///
/// Python mocks `Path.home()` and clears `os.environ` down to `PATH` and a
/// `SECRET_TOKEN`, then asserts the rendered `EnvironmentVariables` dict is
/// exactly `{HOME, PATH}`. Rust's `render` reads `std::env::var("HOME")` /
/// `std::env::var("PATH")` directly (not through an injectable seam), and
/// mutating the real process environment in a parallel `cargo test` binary
/// would race other tests. The structural guarantee -- the function can
/// never emit a third key, because it only ever calls `std::env::var` for
/// these two names (crates/agent-run/src/launchd.rs render) -- is verified
/// here against the ambient test-process environment instead.
#[test]
fn python_capacity_launchd_env_copies_only_home_and_path() {
    let temporary = tempdir().expect("temporary paths");
    let home = temporary.path().join("home");
    let rendered = launchd::render(
        &home,
        std::env::current_exe().expect("test executable"),
        "capacity",
        60,
        "com.example.capacity",
        temporary.path().join("out.log"),
        temporary.path().join("err.log"),
    );
    if cfg!(target_os = "macos") {
        let value = rendered.expect("macOS launchd descriptor");
        let plist = value["plist"].as_str().expect("plist text");
        let start = plist
            .find("EnvironmentVariables")
            .expect("environment block present");
        let end = start + plist[start..].find("</dict>").expect("dict closes");
        let block = &plist[start..end];
        let keys: Vec<&str> = block
            .match_indices("<key>")
            .map(|(index, _)| {
                let rest = &block[index + "<key>".len()..];
                &rest[..rest.find("</key>").expect("key closes")]
            })
            .collect();
        assert!(
            keys.iter().all(|key| *key == "HOME" || *key == "PATH"),
            "unexpected environment key leaked into the plist: {keys:?}"
        );
        assert!(keys.contains(&"HOME"), "HOME must always be present");
    } else {
        assert!(rendered.is_err(), "Linux must explicitly reject launchd");
    }
}
