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

/// Resolves the broker socket, runs the terminal loop, and restores the tty.
async fn observe(cli: Cli) -> agent_run::Result<()> {
    // The palette is chosen once from the terminal's color depth.
    ui::theme::set_palette(ui::theme::palette_for(
        std::env::var("COLORTERM").ok().as_deref(),
    ));
    let home = fs::home(cli.home.clone())?;
    let socket = cli.socket.clone().unwrap_or_else(|| home.join("api.sock"));
    let broker: net::SharedBroker = std::sync::Arc::new(net::SocketBroker::new(socket));
    let mut app = app::App::new();
    app.home_prefix = fs::home(None).ok().map(|path| path.display().to_string());
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
