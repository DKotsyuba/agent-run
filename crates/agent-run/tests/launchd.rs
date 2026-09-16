//! LaunchAgent descriptor parity checks; these never invoke `launchctl`.

use agent_run::launchd;
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
