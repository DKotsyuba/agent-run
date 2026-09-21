//! Focused workspace-root contract tests: legacy singular input, the plural
//! array, ambiguous duplicate declarations, adapter scoping, and the
//! `workspace_network` dependency on at least one configured root.

use agent_run_config::config::Config;
use std::path::Path;

/// Loads one config.toml written with `${HOME}` substituted, returning the
/// loader outcome for the codex runtime's workspace declaration.
fn load(text: &str) -> Result<Vec<String>, agent_run_domain::Error> {
    let temp = tempfile::tempdir().expect("temp dir");
    let home = temp.path().canonicalize().unwrap();
    let full = text.replace("${HOME}", &home.to_string_lossy());
    std::fs::write(home.join("config.toml"), full).unwrap();
    let config = Config::load(&home)?;
    let runtime = config.runtime("codex")?;
    Ok(runtime
        .workspace_roots
        .iter()
        .map(|p| p.to_string_lossy().into_owned())
        .collect())
}

fn codex(extra: &str) -> String {
    format!(
        "schema_version = 1\n[runtimes.codex]\nenabled = true\nadapter = \"codex\"\nbinary = \"/bin/true\"\nhome = \"${{HOME}}/codex\"\nmodels = [\"fixture\"]\n{extra}"
    )
}

/// The legacy singular declaration normalizes to one configured root and is
/// `~`-expanded like every other config path.
#[test]
fn legacy_singular_workspace_root_normalizes_to_one_root() {
    let roots = load(&codex("workspace_root = \"${HOME}/projects\"\n")).unwrap();
    assert_eq!(roots.len(), 1);
    assert!(roots[0].ends_with("/projects"));
    assert!(Path::new(&roots[0]).is_absolute());
}

/// The plural declaration is accepted in declaration order.
#[test]
fn plural_workspace_roots_are_accepted() {
    let roots = load(&codex(
        "workspace_roots = [\"${HOME}/projects\", \"${HOME}/.codex/worktrees\"]\n",
    ))
    .unwrap();
    assert_eq!(roots.len(), 2);
    assert!(roots[0].ends_with("/projects"));
    assert!(roots[1].ends_with("worktrees"));
}

/// Declaring both the singular and plural forms is an ambiguous error.
#[test]
fn duplicate_singular_and_plural_declarations_are_rejected() {
    assert!(load(&codex(
        "workspace_root = \"${HOME}/projects\"\nworkspace_roots = [\"${HOME}/worktrees\"]\n"
    ))
    .is_err());
}

/// Workspace roots remain a codex-only feature.
#[test]
fn workspace_roots_require_codex() {
    let text = codex("workspace_roots = [\"${HOME}/projects\"]\n")
        .replace("adapter = \"codex\"", "adapter = \"claude\"");
    assert!(load(&text).is_err());
}

/// `workspace_network` stays invalid without at least one configured root.
#[test]
fn workspace_network_requires_a_workspace_root() {
    assert!(load(&codex("workspace_network = true\n")).is_err());
    assert!(load(&codex("workspace_roots = []\nworkspace_network = true\n")).is_err());
    assert!(load(&codex(
        "workspace_roots = [\"${HOME}/projects\"]\nworkspace_network = true\n"
    ))
    .is_ok());
}
