//! Quota presentation left Run in 0.32.0: the former `models`, `limits` and
//! `capacity order` CLI commands are unknown, while `delegation-guide` and
//! the owner-proof `history-pins` commands parse.

use agent_run::cli::Cli;
use clap::Parser;

/// Parses CLI arguments from command fragments.
fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
    Cli::try_parse_from(std::iter::once("agent-run").chain(args.iter().copied()))
}

/// The removed quota commands are unknown subcommands, never silent no-ops.
#[test]
fn removed_quota_commands_are_unknown() {
    for args in [
        &["limits"][..],
        &["models"],
        &["models", "--provider", "codex"],
        &["capacity", "order"],
    ] {
        let error = parse(args).expect_err("removed command must not parse");
        assert_eq!(
            error.kind(),
            clap::error::ErrorKind::InvalidSubcommand,
            "{args:?}"
        );
    }
    assert!(parse(&["delegation-guide", "--provider", "codex"]).is_ok());
}

/// Online history pin release requires a credential source and rejects
/// the old request-id proof or a token passed directly on the command line.
#[test]
fn history_pin_release_requires_a_token_file_or_stdin() {
    let pin = "pin_550e8400e29b41d4a716446655440000";
    assert!(parse(&["history-pins", "release", "--pin-id", pin]).is_err());
    for (flag, value) in [("--request-id", "batch-1"), ("--release-token", "token")] {
        assert!(parse(&["history-pins", "release", "--pin-id", pin, flag, value]).is_err());
    }
    assert!(
        parse(&[
            "history-pins",
            "release",
            "--pin-id",
            pin,
            "--release-token-file",
            "-"
        ])
        .is_ok()
    );
    assert!(parse(&["history-pins", "list", "--status", "active"]).is_ok());
}
