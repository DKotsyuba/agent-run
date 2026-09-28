//! `agent-run-tui` — interactive terminal observer for the agent-run broker.
//!
//! The observer is a pure broker client: it starts nothing, owns no store,
//! and mutates nothing. Sessions and transcripts are read through the same
//! public JSON-RPC surface the CLI and MCP transports use.

mod app;
mod events;
mod net;
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
    /// agent-run home directory; defaults to `AGENT_RUN_HOME` or the platform default.
    #[arg(long, env = "AGENT_RUN_HOME")]
    home: Option<PathBuf>,
    /// Broker socket path override; defaults to `<home>/api.sock`.
    #[arg(long)]
    socket: Option<PathBuf>,
}

/// Start the observer runtime and report setup or broker-loop failures on stderr.
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

/// Resolves the broker socket, runs the terminal loop, and restores the tty.
async fn observe(cli: Cli) -> agent_run::Result<()> {
    let home = fs::home(cli.home)?;
    let socket = cli.socket.unwrap_or_else(|| home.join("api.sock"));
    let broker: net::SharedBroker = std::sync::Arc::new(net::SocketBroker::new(socket));
    let mut app = app::App::new();
    app.home_prefix = fs::home(None).ok().map(|path| path.display().to_string());

    let mut guard = events::TerminalGuard::enter()
        .map_err(|error| agent_run::Error::Runtime(format!("terminal setup failed: {error}")))?;
    let result = events::run(app, broker, guard.terminal()).await;
    drop(guard);
    result
}
