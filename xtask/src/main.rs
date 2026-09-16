//! Repository-local verification and sealed native release entry point.
use std::{env, path::PathBuf, process::Command};
use xtask::{deploy, release};

/// Runs the Rust workspace's formatter, linter, and test gates.
fn main() {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    if arguments.first().map(String::as_str) == Some("release") {
        release_command(&arguments[1..]);
        return;
    }
    if arguments.first().map(String::as_str) != Some("check") {
        eprintln!(
            "usage: cargo xtask check | release build|build-native|verify|install|update|rollback"
        );
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

/// Builds the native binary for an installed Cargo target and returns its path.
///
/// This is intentionally offline: target support is the set installed in the
/// invoking toolchain, and a missing target fails before an incomplete release
/// directory is created.
fn native_binary(target: Option<&str>) -> Result<PathBuf, String> {
    let mut command = Command::new("cargo");
    command.args(["build", "--offline", "--release", "-p", "agent-run"]);
    if let Some(target) = target {
        command.args(["--target", target]);
    }
    if !command
        .status()
        .map_err(|error| error.to_string())?
        .success()
    {
        return Err("native Cargo build failed".into());
    }
    let directory = env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target"));
    let directory = target
        .map(|target| directory.join(target))
        .unwrap_or(directory);
    Ok(directory.join("release/agent-run"))
}

/// Parses the deliberately small offline release/deployment command surface.
fn release_command(arguments: &[String]) {
    let value = |name: &str| {
        arguments
            .windows(2)
            .find(|pair| pair[0] == name)
            .map(|pair| pair[1].clone())
            .map(PathBuf::from)
    };
    let text = |name: &str| {
        arguments
            .windows(2)
            .find(|pair| pair[0] == name)
            .map(|pair| pair[1].clone())
    };
    let force = arguments.iter().any(|argument| argument == "--force");
    let result = match arguments.first().map(String::as_str) {
        Some("build") => release::build(
            &value("--output")
                .ok_or("--output is required")
                .unwrap_or_default(),
            &text("--version").unwrap_or_default(),
            &value("--binary")
                .ok_or("--binary is required")
                .unwrap_or_default(),
        )
        .map(|path| println!("{}", path.display())),
        Some("build-native") => native_binary(text("--target").as_deref())
            .and_then(|binary| {
                release::build(
                    &value("--output").ok_or("--output is required")?,
                    &text("--version").ok_or("--version is required")?,
                    &binary,
                )
            })
            .map(|path| println!("{}", path.display())),
        Some("verify") => release::verify(
            &value("--release")
                .ok_or("--release is required")
                .unwrap_or_default(),
        ),
        Some("install") | Some("update") => deploy::deploy(
            &value("--prefix")
                .ok_or("--prefix is required")
                .unwrap_or_default(),
            &value("--home")
                .ok_or("--home is required")
                .unwrap_or_default(),
            &value("--release")
                .ok_or("--release is required")
                .unwrap_or_default(),
            force,
        ),
        Some("rollback") => deploy::rollback(
            &value("--prefix")
                .ok_or("--prefix is required")
                .unwrap_or_default(),
            &value("--home")
                .ok_or("--home is required")
                .unwrap_or_default(),
            force,
        ),
        _ => Err(
            "usage: cargo xtask release build|build-native|verify|install|update|rollback".into(),
        ),
    };
    if let Err(error) = result {
        eprintln!("release stopped: {error}");
        std::process::exit(2);
    }
}
