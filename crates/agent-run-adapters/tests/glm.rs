//! GLM launch model-marker regressions.

/// Known million-context GLM aliases retain Claude Code's required suffix.
#[test]
fn million_context_models_get_the_claude_suffix() {
    assert_eq!(agent_run_adapters::glm::cli_model("glm-5.3"), "glm-5.3[1m]");
    assert_eq!(
        agent_run_adapters::glm::cli_model("glm-5.3-flash"),
        "glm-5.3-flash[1m]"
    );
    assert_eq!(agent_run_adapters::glm::cli_model("other"), "other");
}
