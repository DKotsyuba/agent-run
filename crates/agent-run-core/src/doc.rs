//! Embedded operator-guide pages shared by the CLI and MCP transports.

use crate::{error::invalid, Result};

/// Returns one complete, build-time embedded operator-guide page.
///
/// `topic` must be `index` or one of the public guide topics.  The returned
/// string is static binary data, so it remains available after installation
/// without a source checkout or a dependency on the caller's working
/// directory. The generated `completion` topic omits its packaged terminal
/// newline, matching the same contract embedded in start-tool discovery.
pub fn topic_text(topic: &str) -> Result<&'static str> {
    match topic {
        "index" => Ok(include_str!("../../../assets/operator_guide/index.md")),
        // Python composes this generated contract without the source file's
        // terminal newline, and the start tool embeds that exact text.
        "completion" => Ok(include_str!("../../../assets/operator_guide/completion.md").trim_end()),
        "config" => Ok(include_str!("../../../assets/operator_guide/config.md")),
        "skills" => Ok(include_str!("../../../assets/operator_guide/skills.md")),
        "mcp-servers" => Ok(include_str!("../../../assets/operator_guide/mcp-servers.md")),
        "plugins" => Ok(include_str!("../../../assets/operator_guide/plugins.md")),
        "models" => Ok(include_str!("../../../assets/operator_guide/models.md")),
        "releases" => Ok(include_str!("../../../assets/operator_guide/releases.md")),
        "migrations" => Ok(include_str!("../../../assets/operator_guide/migrations.md")),
        "troubleshoot" => Ok(include_str!("../../../assets/operator_guide/troubleshoot.md")),
        _ => Err(invalid(format!(
            "unknown operator guide topic: {topic:?}; valid topics: completion, config, skills, mcp-servers, plugins, models, releases, migrations, troubleshoot"
        ))),
    }
}
