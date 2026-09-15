//! Repository-local verification entry point with no runtime dependencies.
use std::{env, process::Command};

/// Runs the Rust workspace's formatter, linter, and test gates.
fn main() {
    if env::args().nth(1).as_deref() != Some("check") {
        eprintln!("usage: cargo xtask check");
        std::process::exit(2);
    }
    for arguments in [
        vec!["fmt", "--all", "--check"],
        vec![
            "clippy",
            "--offline",
            "--workspace",
            "--all-targets",
            "--all-features",
        ],
        vec!["test", "--offline", "--workspace", "--all-features"],
    ] {
        let status = Command::new("cargo")
            .args(arguments)
            .status()
            .expect("cargo must be executable");
        if !status.success() {
            std::process::exit(status.code().unwrap_or(1));
        }
    }
}
