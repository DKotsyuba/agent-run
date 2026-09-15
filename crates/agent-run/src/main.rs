use clap::Parser;
#[tokio::main]
async fn main() {
    let code = match agent_run::cli::run(agent_run::cli::Cli::parse()).await {
        Ok(code) => code,
        Err(error) => {
            let value = serde_json::json!({"error":error.public()});
            // MCP protocol errors never spill JSON onto the stdio protocol stream.
            eprintln!("{value}");
            2
        }
    };
    std::process::exit(code);
}
