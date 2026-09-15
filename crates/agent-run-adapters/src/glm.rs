//! GLM-specific Claude CLI launch conventions.

const MILLION_CONTEXT_MODELS: &[&str] = &["glm-5.3", "glm-5.3-flash", "glm-5.2"];

/// Adds Claude Code's one-million-token model marker for known GLM models.
///
/// Z.ai exposes these models with a 1,048,576-token context window, but the
/// Claude CLI otherwise treats unknown IDs as 200k-context models. Other
/// model identifiers, including ones already suffixed, are returned unchanged.
pub fn cli_model(model: &str) -> String {
    if MILLION_CONTEXT_MODELS.contains(&model) {
        format!("{model}[1m]")
    } else {
        model.into()
    }
}
