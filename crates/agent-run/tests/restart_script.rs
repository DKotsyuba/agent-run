//! Service-manager wrapper tests with disposable fake commands only.

use std::{fs, os::unix::fs::PermissionsExt, path::Path, process::Command};

/// Write a private fixture command which never touches a real service.
fn executable(path: &Path, script: &str) {
    fs::write(path, script).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
}

/// A manager is called only after readiness; its uncertain failure never
/// requests an automatic defer or claims that recovery completed.
#[test]
fn restart_wrapper_orders_preparation_manager_and_exact_epoch_wait() {
    let root = tempfile::tempdir().unwrap();
    let bin = root.path().join("bin");
    fs::create_dir(&bin).unwrap();
    let log = root.path().join("calls");
    executable(
        &bin.join("uname"),
        "#!/bin/sh\nprintf '%s\\n' \"$FIXTURE_OS\"\n",
    );
    executable(
        &bin.join("agent-run"),
        "#!/bin/sh\nprintf 'agent %s\\n' \"$*\" >> \"$FIXTURE_LOG\"\ncase \"$*\" in *'maintenance prepare --epoch-only') [ \"${FIXTURE_PREPARE:-0}\" = 0 ] || exit 2; echo 7;; *'maintenance wait 7') echo '{\"phase\":\"complete\"}';; *) exit 3;; esac\n",
    );
    executable(
        &bin.join("manager"),
        "#!/bin/sh\nprintf 'manager %s\\n' \"$*\" >> \"$FIXTURE_LOG\"\nexit \"${FIXTURE_MANAGER:-0}\"\n",
    );
    let script =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/services/agent-run-restart.sh");
    for (platform, prepare, manager, success, expected_manager) in [
        (
            "Linux",
            "0",
            "0",
            true,
            "manager restart -- agent-run.service",
        ),
        (
            "Darwin",
            "0",
            "0",
            true,
            "manager kill SIGTERM gui/42/fixture",
        ),
        ("Linux", "1", "0", false, ""),
        (
            "Linux",
            "0",
            "1",
            false,
            "manager restart -- agent-run.service",
        ),
    ] {
        fs::write(&log, b"").unwrap();
        let output = Command::new(&script)
            .args(["--broker-socket", "/fixture/socket"])
            .env("PATH", format!("{}:/usr/bin:/bin", bin.display()))
            .env("AGENT_RUN_BINARY", bin.join("agent-run"))
            .env("AGENT_RUN_SYSTEMCTL", bin.join("manager"))
            .env("AGENT_RUN_LAUNCHCTL", bin.join("manager"))
            .env("AGENT_RUN_LAUNCHD_TARGET", "gui/42/fixture")
            .env("FIXTURE_OS", platform)
            .env("FIXTURE_PREPARE", prepare)
            .env("FIXTURE_MANAGER", manager)
            .env("FIXTURE_LOG", &log)
            .output()
            .unwrap();
        assert_eq!(
            output.status.success(),
            success,
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let lines = fs::read_to_string(&log).unwrap();
        assert!(lines.starts_with(
            "agent --broker-socket /fixture/socket maintenance prepare --epoch-only\n"
        ));
        assert_eq!(
            lines.contains(expected_manager) && !expected_manager.is_empty(),
            prepare == "0"
        );
        assert_eq!(lines.contains("maintenance wait 7"), success);
        assert!(!lines.contains("maintenance defer"));
    }
}
