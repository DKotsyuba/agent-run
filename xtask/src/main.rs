//! Repository-local verification and sealed native release entry point.
use std::{env, path::PathBuf, process::Command};
use xtask::{archive, deploy, evidence, qualify, release};

/// Runs the Rust workspace's formatter plus locked linter and test gates.
fn main() {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    if arguments.first().map(String::as_str) == Some("release") {
        release_command(&arguments[1..]);
        return;
    }
    if arguments.first().map(String::as_str) == Some("archive") {
        archive_command(&arguments[1..]);
        return;
    }
    if arguments.first().map(String::as_str) == Some("evidence") {
        evidence_command(&arguments[1..]);
        return;
    }
    if arguments.first().map(String::as_str) == Some("qualify") {
        qualify_command(&arguments[1..]);
        return;
    }
    if arguments.first().map(String::as_str) != Some("check") {
        eprintln!(
            "usage: cargo xtask check | qualify [--release] | release build|build-native|verify|install|update|recover|roll-forward|rollback | archive --verify | evidence verify"
        );
        std::process::exit(2);
    }
    for arguments in [
        vec!["fmt", "--all", "--check"],
        vec![
            "clippy",
            "--offline",
            "--locked",
            "--workspace",
            "--all-targets",
            "--all-features",
            "--",
            "-D",
            "warnings",
        ],
        vec![
            "test",
            "--offline",
            "--locked",
            "--workspace",
            "--all-features",
        ],
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

/// Checks the current host against immutable recorded qualification evidence.
fn qualify_command(arguments: &[String]) {
    let release = match arguments {
        [] => false,
        [argument] if argument == "--release" => true,
        _ => {
            eprintln!("qualify stopped: usage: cargo xtask qualify [--release]");
            std::process::exit(2);
        }
    };
    let root = env::current_dir().expect("current directory must be readable");
    match qualify::qualify(&root, &qualify::host_platform(), release) {
        Ok(report) => print!("{report}"),
        Err(error) => {
            eprintln!("qualify stopped: {error}");
            std::process::exit(2);
        }
    }
}

/// Builds a source archive for a revision and optionally verifies its contents.
fn archive_command(arguments: &[String]) {
    let value = |name: &str| {
        arguments
            .windows(2)
            .find(|pair| pair[0] == name)
            .map(|pair| PathBuf::from(&pair[1]))
    };
    let revision = arguments
        .windows(2)
        .find(|pair| pair[0] == "--revision")
        .map(|pair| pair[1].as_str())
        .unwrap_or("HEAD");
    let root = env::current_dir().expect("current directory must be readable");
    let default = archive::default_output(&root, revision);
    let output = value("--output");
    let result = default
        .and_then(|default| archive::build(&root, output.as_deref().unwrap_or(&default), revision))
        .and_then(|path| {
            let verify = arguments.iter().any(|argument| argument == "--verify");
            if verify {
                archive::verify(&root, &path)?;
            }
            println!(
                "archive {}: {}",
                if verify { "verified" } else { "created" },
                path.display()
            );
            Ok(())
        });
    if let Err(error) = result {
        eprintln!("archive stopped: {error}");
        std::process::exit(2);
    }
}

/// Verifies the repository's hand-assembled migration evidence index.
fn evidence_command(arguments: &[String]) {
    let result = match arguments.first().map(String::as_str) {
        Some("verify") if arguments.len() == 1 => {
            let root = env::current_dir().expect("current directory must be readable");
            evidence::verify(&root)
                .map(|count| println!("evidence verify: passed ({count} entries)"))
        }
        _ => Err("usage: cargo xtask evidence verify".into()),
    };
    if let Err(error) = result {
        eprintln!("evidence stopped: {error}");
        std::process::exit(2);
    }
}

/// Builds the native binary for an installed Cargo target and returns its path.
///
/// This is intentionally offline and locked: target support is the set installed
/// in the invoking toolchain, dependency resolution cannot modify `Cargo.lock`,
/// and a missing target fails before an incomplete release directory is created.
fn native_binary(target: Option<&str>) -> Result<PathBuf, String> {
    let mut command = Command::new("cargo");
    command.args([
        "build",
        "--offline",
        "--locked",
        "--release",
        "-p",
        "agent-run",
    ]);
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
        Some("recover") => deploy::recover(
            &value("--prefix")
                .ok_or("--prefix is required")
                .unwrap_or_default(),
            &value("--home")
                .ok_or("--home is required")
                .unwrap_or_default(),
            force,
        ),
        Some("roll-forward") => deploy::roll_forward(
            &value("--prefix")
                .ok_or("--prefix is required")
                .unwrap_or_default(),
            &value("--home")
                .ok_or("--home is required")
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
            "usage: cargo xtask release build|build-native|verify|install|update|recover|roll-forward|rollback".into(),
        ),
    };
    if let Err(error) = result {
        eprintln!("release stopped: {error}");
        std::process::exit(2);
    }
}
