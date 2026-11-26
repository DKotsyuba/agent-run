//! Plain-text delegation guide templated from one committed catalog snapshot.
//!
//! The layout lives in the repo-owned MiniJinja asset
//! `assets/delegation_guide.txt.j2`, embedded at build time (never loaded
//! from the filesystem or hot-reloaded). This module only prepares a small
//! normalized projection of one committed provider catalog snapshot in
//! configured order and renders it with any supplied paid-credit fallback notes.
//! It adds no ranking of model ability and no
//! account, credential, endpoint, or hash detail; configured recommendation
//! prose is whitespace-normalized and never invented.

use crate::{Error, Result};
use minijinja::{AutoEscape, Environment, UndefinedBehavior};
use serde_json::{Value, json};
use std::sync::OnceLock;

/// The one embedded guide template; a repo asset, not a configuration knob.
const TEMPLATE: &str = include_str!("../../../assets/delegation_guide.txt.j2");

/// Returns the process-wide single-template rendering environment.
///
/// Block tags trim their surrounding newlines, the template's own trailing
/// newline is kept, undefined values are strict (a projection/template
/// mismatch fails loudly instead of printing nothing), and auto-escaping is
/// explicitly off because the guide is plain text. Template registration can
/// only fail for an invalid repo-owned asset, which is a build-time
/// programming defect (mirroring the tool-registry parse), never a caller
/// condition.
fn environment() -> &'static Environment<'static> {
    static ENVIRONMENT: OnceLock<Environment> = OnceLock::new();
    ENVIRONMENT.get_or_init(|| {
        let mut environment = Environment::new();
        environment.set_trim_blocks(true);
        environment.set_lstrip_blocks(true);
        environment.set_keep_trailing_newline(true);
        environment.set_undefined_behavior(UndefinedBehavior::Strict);
        environment.set_auto_escape_callback(|_| AutoEscape::None);
        environment
            .add_template("delegation_guide", TEMPLATE)
            .expect("delegation guide template asset is valid");
        environment
    })
}

/// Renders the guide text for one provider catalog snapshot.
///
/// `catalog` must be the value
/// [`crate::capacity::provider_catalog::catalog_at`] returned for the
/// caller's exact filters (or default query): providers in configured order
/// with each model's admissible profiles, configured params, restrictions,
/// and recommendation prose. The template owns prose and conditional omission
/// over the compact projection built here; a model no canonical profile may
/// use says `profiles: none`, and an explicitly empty catalog says so.
pub fn render(catalog: &Value) -> Result<String> {
    let providers: Vec<Value> = catalog["providers"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|provider| {
            let models: Vec<Value> = provider["models"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|model| {
                    let profiles = strings(&model["profiles"]).join(", ");
                    json!({
                        "id": model["model"].as_str().unwrap_or("unknown"),
                        "profiles": if profiles.is_empty() { "none".to_owned() } else { profiles },
                        "params": params(&model["params"], &model["allowed_params"]),
                        "restrictions": nonempty(&strings(&model["restrictions"]).join(", ")),
                        "guidance": guidance(&model["recommendations"]),
                        "credit_backed": model["credit_backed"].as_bool().unwrap_or(false),
                    })
                })
                .collect();
            // When every model admits the same nonempty profile set, state it
            // once at the provider instead of repeating it per model.
            let common = models
                .iter()
                .map(|model| model["profiles"].as_str().unwrap_or("none"))
                .reduce(|left, right| if left == right { left } else { "" })
                .filter(|common| !common.is_empty() && *common != "none")
                .map(str::to_owned);
            let mut provider = json!({
                "id": provider["provider"].as_str().unwrap_or("unknown"),
                "harness": provider["harness"].as_str().unwrap_or("unknown"),
                "guidance": guidance(&provider["recommendations"]),
                "models": models,
            });
            // Null (not removal) so strict-undefined templates still see a
            // defined, falsy key after hoisting.
            provider["profiles"] = common
                .as_deref()
                .map_or(Value::Null, |common| json!(common));
            if common.is_some() {
                for model in provider["models"].as_array_mut().into_iter().flatten() {
                    model["profiles"] = Value::Null;
                }
            }
            provider
        })
        .collect();
    environment()
        .get_template("delegation_guide")
        .map_err(|error| Error::Runtime(format!("delegation guide template is missing: {error}")))?
        .render(json!({ "providers": providers }))
        .map_err(|error| Error::Runtime(format!("delegation guide render failed: {error}")))
}

/// Returns the whitespace-normalized configured recommendation strings.
///
/// Empty or absent arrays stay empty: the template then omits the guidance
/// lines entirely, so no fabricated description is ever emitted.
fn guidance(recommendations: &Value) -> Vec<String> {
    strings(recommendations)
        .iter()
        .map(|entry| prose(entry))
        .collect()
}

/// Renders configured default and allowed params as one compact fragment,
/// or `None` when neither side has any entry (the template then omits it).
fn params(defaults: &Value, allowed: &Value) -> Option<String> {
    let mut parts = Vec::new();
    for (name, value) in defaults.as_object().into_iter().flatten() {
        parts.push(format!("{name}={}", scalar(value)));
    }
    for (name, values) in allowed.as_object().into_iter().flatten() {
        let choices = strings(values).join("|");
        if choices.is_empty() {
            parts.push(format!("allowed {name}"));
        } else {
            parts.push(format!("allowed {name}: {choices}"));
        }
    }
    nonempty(&parts.join("; "))
}

/// Returns `None` for an empty string so templates can omit empty lines.
fn nonempty(text: &str) -> Option<String> {
    (!text.is_empty()).then(|| text.to_owned())
}

/// Renders one JSON scalar without JSON quoting.
fn scalar(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Bool(flag) => flag.to_string(),
        Value::Number(number) => number.to_string(),
        _ => value.to_string(),
    }
}

/// Collects the string entries of a JSON array of strings.
fn strings(value: &Value) -> Vec<String> {
    value
        .as_array()
        .map(|entries| {
            entries
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

/// Normalizes configured prose to single spaces.
///
/// Every control or whitespace character (newlines, tabs, and the rest)
/// collapses into one plain space; all other Unicode text is preserved
/// verbatim, so multi-line or control-laden configuration cannot forge
/// guide layout.
fn prose(text: &str) -> String {
    text.chars()
        .map(|character| {
            if character.is_control() || character.is_whitespace() {
                ' '
            } else {
                character
            }
        })
        .collect::<String>()
        .split(' ')
        .filter(|word| !word.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::render;
    use serde_json::json;

    /// Empty/control-laden catalogs stay honest and the guide carries no
    /// quota standing or pointer to a removed quota tool.
    #[test]
    fn render_states_an_empty_catalog_and_normalizes_prose() {
        let empty = render(&json!({"providers": []})).unwrap();
        assert!(empty.contains("No providers are currently configured."));
        assert!(!empty.contains("use limits"), "{empty}");
        assert!(!empty.contains("reflects capacity"), "{empty}");
        assert!(empty.contains("Agent Quota"), "{empty}");
        let messy = render(&json!({
            "providers": [{
                "provider": "p", "harness": "codex", "models": [{
                    "model": "m\u{00e9}",
                    "recommendations": ["line one\nline\u{0007}two  spaced"],
                }],
                "recommendations": ["ok"],
            }]
        }))
        .unwrap();
        assert!(messy.contains("- mé\n"), "{messy}");
        assert!(!messy.contains(": quota "), "{messy}");
        assert!(messy.contains("provider guidance: ok"));
        assert!(messy.contains("model guidance: line one line two spaced"));
        assert!(messy.contains("profiles: none"), "{messy}");
    }

    /// Unconfigured params, restrictions, and guidance lines are omitted, not
    /// fabricated, and per-model profiles stay distinct when they differ.
    #[test]
    fn render_omits_unconfigured_lines() {
        let text = render(&json!({
            "providers": [{
                "provider": "p", "harness": "codex", "models": [
                    {"model": "seen", "profiles": ["code"]},
                    {"model": "spent", "profiles": []},
                ],
            }]
        }))
        .unwrap();
        assert!(text.contains("- seen\n"), "{text}");
        assert!(text.contains("profiles: code"), "{text}");
        assert!(text.contains("profiles: none"), "{text}");
        assert!(!text.contains("params:"), "{text}");
        assert!(!text.contains("restrictions:"), "{text}");
        assert!(!text.contains(": quota "), "{text}");
        assert!(!text.contains("evidence"), "{text}");
        assert!(text.ends_with('\n'), "{text}");
    }
}
