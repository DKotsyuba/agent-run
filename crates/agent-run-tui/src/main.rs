//! `agent-run-tui` — interactive terminal observer for the agent-run broker.
//!
//! The observer is a pure broker client: it starts nothing, owns no store,
//! and never mutates broker state. Explicit copy writes only the local clipboard.
//! Sessions and transcripts are read through the same
//! public JSON-RPC surface the CLI and MCP transports use.

mod app;
mod clipboard;
mod events;
mod net;
mod pools;
mod ui;

#[cfg(test)]
mod tests_support;

use agent_run_platform::fs;
use clap::Parser;
use std::path::PathBuf;

/// Command line of the terminal observer.
#[derive(Debug, Parser)]
#[command(
    name = "agent-run-tui",
    about = "Interactive terminal observer for the agent-run resident broker",
    version
)]
struct Cli {
    /// Project directory to observe: shows only sessions of that project,
    /// its checkout and every worktree (`<dir>/.claude/worktrees/*`,
    /// `<dir>/.worktrees/*`, `<dir>/worktrees/*`).
    project: Option<PathBuf>,
    /// agent-run home directory; defaults to `AGENT_RUN_HOME` or the platform default.
    #[arg(long, env = "AGENT_RUN_HOME")]
    home: Option<PathBuf>,
    /// Broker socket path override; defaults to `<home>/api.sock`.
    #[arg(long)]
    socket: Option<PathBuf>,
    /// Expected broker server UID; requires an explicit socket.
    #[arg(long, requires = "socket")]
    broker_uid: Option<u32>,
    /// Open a cooperative pool directly in the read-only Pools tab.
    #[arg(long)]
    pool: Option<agent_run_domain::pool::PoolId>,
}

fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("agent-run-tui: tokio runtime failed: {error}");
            return std::process::ExitCode::FAILURE;
        }
    };
    match runtime.block_on(observe(cli)) {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("agent-run-tui: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}

/// Resolves the requested project directory to its canonical root.
fn project_root(cli: &Cli) -> agent_run::Result<Option<String>> {
    let Some(project) = &cli.project else {
        return Ok(None);
    };
    // Shell-level `~` survives inside quotes; expand it against the home.
    let path = match project.to_str() {
        Some(text) if text.starts_with("~/") && cli.socket.is_some() => {
            fs::expand(project).unwrap_or_else(|_| project.clone())
        }
        Some(text) if text.starts_with("~/") => match fs::home(None) {
            Ok(home) => home.join(&text[2..]),
            Err(_) => project.clone(),
        },
        _ => project.clone(),
    };
    let root = std::fs::canonicalize(&path)
        .map_err(|_| agent_run::Error::NotFound(path.display().to_string()))?
        .display()
        .to_string();
    Ok(Some(root))
}

/// Resolves the explicit socket without consulting the broker home.
fn broker_socket(cli: &Cli) -> agent_run::Result<PathBuf> {
    match &cli.socket {
        Some(socket) => Ok(socket.clone()),
        None => Ok(fs::home(cli.home.clone())?.join("api.sock")),
    }
}

/// Resolves the broker socket, runs the terminal loop, and restores the tty.
async fn observe(cli: Cli) -> agent_run::Result<()> {
    // The palette is chosen once from the terminal's color depth.
    ui::theme::set_palette(ui::theme::palette_for(
        std::env::var("COLORTERM").ok().as_deref(),
    ));
    let socket = broker_socket(&cli)?;
    let broker: net::SharedBroker = std::sync::Arc::new(match cli.broker_uid {
        Some(uid) => net::SocketBroker::with_expected_server_uid(socket, Some(uid)),
        None => net::SocketBroker::new(socket),
    });
    let mut app = app::App::new();
    if cli.socket.is_none() {
        app.home_prefix = fs::home(None).ok().map(|path| path.display().to_string());
    }
    app.project_filter = project_root(&cli)?;
    if let Some(id) = cli.pool {
        app.pools.visible = true;
        app.pools.focused = true;
        app.pools.select(id);
    }

    let mut guard = events::TerminalGuard::enter()
        .map_err(|error| agent_run::Error::Runtime(format!("terminal setup failed: {error}")))?;
    let result = events::run(app, broker, guard.terminal()).await;
    drop(guard);
    result
}

#[cfg(test)]
mod cli_tests {
    use super::*;

    /// An explicit UID requires a socket and must be a valid unsigned UID.
    #[test]
    fn broker_uid_requires_socket_and_valid_uid() {
        assert!(Cli::try_parse_from(["tui", "--broker-uid", "123"]).is_err());
        assert!(
            Cli::try_parse_from(["tui", "--socket", "/run/broker.sock", "--broker-uid", "-1"])
                .is_err()
        );
        let cli =
            Cli::try_parse_from(["tui", "--socket", "/run/broker.sock", "--broker-uid", "123"])
                .unwrap();
        assert_eq!(cli.broker_uid, Some(123));
    }

    /// An explicit socket is resolved even when the configured home is invalid.
    #[test]
    fn explicit_socket_does_not_resolve_home() {
        let mut cli = Cli::try_parse_from(["tui", "--socket", "/run/broker.sock"]).unwrap();
        // Clap rejects a blank argument, but a programmatic caller can still
        // supply one; the explicit socket must not resolve it.
        cli.home = Some(PathBuf::new());
        assert_eq!(
            broker_socket(&cli).unwrap(),
            PathBuf::from("/run/broker.sock")
        );
        cli.socket = None;
        assert!(broker_socket(&cli).is_err());
    }
}
