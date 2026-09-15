use clap::{error::ErrorKind, Parser};

/// Parses and executes the operator CLI with JSON errors compatible with Python.
#[tokio::main]
async fn main() {
    let cli = match agent_run::cli::Cli::try_parse() {
        Ok(cli) => cli,
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::DisplayHelp | ErrorKind::DisplayVersion
            ) =>
        {
            let _ = error.print();
            std::process::exit(0);
        }
        Err(error) => {
            eprintln!(
                "{}",
                serde_json::json!({"error":{"type":"ValidationError","message":error.to_string().chars().take(512).collect::<String>()}})
            );
            std::process::exit(2);
        }
    };
    let code = match agent_run::cli::run(cli).await {
        Ok(code) => code,
        Err(error) => {
            let public = error.public();
            let value = serde_json::json!({"error":{"type":public.kind,"message":public.message}});
            // MCP protocol errors never spill JSON onto the stdio protocol stream.
            eprintln!("{value}");
            error.protocol_mapping().cli_exit_code
        }
    };
    std::process::exit(code);
}
