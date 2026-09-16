//! Claude-family runtime validation shared by Claude and GLM.

use agent_run_config::config::{Adapter, Runtime};
use agent_run_domain::{error::invalid, Result};
use std::path::Path;

const KNOWN_HOOK_EVENTS: &[&str] = &[
    "PreToolUse",
    "PermissionRequest",
    "PostToolUse",
    "PostToolUseFailure",
    "PreCompact",
    "PostCompact",
    "SessionStart",
    "UserPromptSubmit",
    "SubagentStart",
    "SubagentStop",
    "SessionEnd",
    "Stop",
];

/// Rejects Claude hook names and GLM whole-plugin skills that Python rejects.
///
/// Claude Code accepts only the documented hook event names. GLM loads every
/// declared plugin as a whole, so every plugin skill with a `SKILL.md` must be
/// explicitly listed in `runtime.skills`; otherwise a role could receive an
/// undeclared prompt. Other adapters have different plugin loading semantics
/// and are intentionally not checked here.
pub fn validate_runtime(runtime: &Runtime, kind: Adapter) -> Result<()> {
    if !matches!(kind, Adapter::Claude | Adapter::Glm) {
        return Ok(());
    }
    if runtime
        .hooks
        .iter()
        .any(|hook| !KNOWN_HOOK_EVENTS.contains(&hook.event.as_str()))
    {
        return Err(invalid("unknown Claude hook event"));
    }
    if kind == Adapter::Glm {
        for plugin in &runtime.plugins {
            let skills = plugin.join("skills");
            let entries = match std::fs::read_dir(&skills) {
                Ok(entries) => entries,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            for entry in entries {
                let entry = entry?;
                let name = entry.file_name().to_string_lossy().into_owned();
                if Path::new(&entry.path()).join("SKILL.md").is_file()
                    && !runtime.skills.contains(&name)
                {
                    return Err(invalid(
                        "glm plugin skill is not declared in runtime skills",
                    ));
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::path::PathBuf;

    /// Builds only the runtime fields relevant to adapter-owned validation.
    fn runtime(kind: &str, plugins: Vec<PathBuf>, skills: Vec<&str>) -> Runtime {
        serde_json::from_value(json!({
            "enabled": true,
            "adapter": kind,
            "binary": "/bin/true",
            "home": "/tmp/runtime-home",
            "models": ["fixture"],
            "plugins": plugins,
            "skills": skills,
        }))
        .expect("test runtime")
    }

    /// Mirrors `test_claude_adapter.py::test_unknown_hook_event_is_rejected`.
    #[test]
    fn rejects_unknown_claude_hook_event() {
        let mut runtime = runtime("claude", vec![], vec![]);
        runtime.hooks.push(agent_run_config::config::Hook {
            event: "UnknownEvent".into(),
            command: vec!["true".into()],
            matcher: None,
        });
        assert!(validate_runtime(&runtime, Adapter::Claude).is_err());
    }

    /// Mirrors `test_glm_adapter.py::test_unlisted_plugin_skills_are_rejected`.
    #[test]
    fn glm_rejects_plugin_skills_missing_from_runtime_declaration() {
        let root = std::env::temp_dir().join(format!("agent-run-glm-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(root.join("skills/undeclared")).expect("plugin skill directory");
        std::fs::write(root.join("skills/undeclared/SKILL.md"), "fixture").expect("plugin skill");
        let runtime = runtime("glm", vec![root.clone()], vec![]);
        assert!(validate_runtime(&runtime, Adapter::Glm).is_err());
        std::fs::remove_dir_all(root).expect("remove owned temporary plugin");
    }
}
