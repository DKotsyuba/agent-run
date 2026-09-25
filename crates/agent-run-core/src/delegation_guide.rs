//! Plain-text delegation guide templated from one committed catalog snapshot.
//!
//! The layout lives in the repo-owned MiniJinja asset
//! `assets/delegation_guide.txt.j2`, embedded at build time (never loaded
//! from the filesystem or hot-reloaded). This module only prepares a small
//! normalized projection of the exact JSON the public `models` read already
//! produced — one committed provider/quota snapshot in capacity order — and
//! renders it. It adds no facts, no collection, no ranking of model ability,
//! and no account, credential, endpoint, or hash detail; configured
//! recommendation prose is whitespace-normalized and never invented.

use crate::{Error, Result};
use minijinja::{AutoEscape, Environment, UndefinedBehavior};
use serde_json::{json, Value};
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

/// Renders the guide text for one `models`-shaped catalog snapshot.
///
/// `catalog` must be the value [`crate::capacity::provider_catalog::models`]
/// returned for the default (unfiltered) query: providers already stand in
/// capacity order with each model's cached quota standing, admissible
/// profiles, configured params, restrictions, and recommendation prose. The
/// template owns prose and conditional omission over the compact projection
/// built here; unknown, stale, or missing standings keep their own words —
/// never rendered as healthy — a model no canonical profile may use says
/// `profiles: none`, and an explicitly empty catalog says so. Sample ages
/// and reset horizons are derived from the snapshot's own `ranked_at`
/// advice clock, never printed as raw epochs.
pub fn render(catalog: &Value) -> Result<String> {
    let ranked_at = catalog["ranked_at"].as_f64();
    let providers: Vec<Value> = catalog["providers"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|provider| {
            json!({
                "id": provider["provider"].as_str().unwrap_or("unknown"),
                "harness": provider["harness"].as_str().unwrap_or("unknown"),
                "guidance": guidance(&provider["recommendations"]),
                "models": provider["models"].as_array().into_iter().flatten().map(|model| {
                    let profiles = strings(&model["profiles"]).join(", ");
                    json!({
                        "id": model["model"].as_str().unwrap_or("unknown"),
                        "status": model["quota"]["status"].as_str().unwrap_or("unknown"),
                        "evidence": model["quota"]["evidence"].as_str().unwrap_or("missing"),
                        "resets": ranked_at
                            .zip(model["quota"]["exhausted_until"].as_f64())
                            .map(|(ranked_at, until)| span(until - ranked_at)),
                        "age": ranked_at
                            .zip(model["quota"]["newest_observed_at"].as_f64())
                            .map(|(ranked_at, at)| span(ranked_at - at)),
                        "profiles": if profiles.is_empty() { "none".to_owned() } else { profiles },
                        "params": params(&model["params"], &model["allowed_params"]),
                        "restrictions": nonempty(&strings(&model["restrictions"]).join(", ")),
                        "guidance": guidance(&model["recommendations"]),
                    })
                }).collect::<Vec<_>>(),
            })
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

/// Renders one finite non-negative duration in compact human units.
///
/// Sub-second spans round up to `1s`; anything from seconds upward keeps
/// its largest whole unit (`45s`, `2m`, `2h`, `3d`), and longer spans stay
/// in days. A non-finite or negative input renders as `0s` rather than a
/// misleading clock reading.
fn span(seconds: f64) -> String {
    if !seconds.is_finite() || seconds <= 0.0 {
        return "0s".to_owned();
    }
    let whole = [(86_400.0, "d"), (3_600.0, "h"), (60.0, "m"), (1.0, "s")];
    for (unit, suffix) in whole {
        if seconds >= unit {
            return format!("{}{suffix}", (seconds / unit).ceil());
        }
    }
    "1s".to_owned()
}

#[cfg(test)]
mod tests {
    use super::render;
    use serde_json::json;

    /// The guide stays honest for an empty and for a control-laden catalog.
    #[test]
    fn render_states_an_empty_catalog_and_normalizes_prose() {
        let empty = render(&json!({"providers": []})).unwrap();
        assert!(empty.contains("No providers are currently configured."));
        let messy = render(&json!({
            "ranked_at": 1000.0,
            "providers": [{
                "provider": "p", "harness": "codex", "models": [{
                    "model": "m\u{00e9}", "quota": {"status": "unknown", "evidence": "missing"},
                    "recommendations": ["line one\nline\u{0007}two  spaced"],
                }],
                "recommendations": ["ok"],
            }]
        }))
        .unwrap();
        assert!(messy.contains("- mé: quota unknown, evidence missing"));
        assert!(messy.contains("provider guidance: ok"));
        assert!(messy.contains("model guidance: line one line two spaced"));
        assert!(messy.contains("profiles: none"), "{messy}");
    }

    /// Sample ages and reset horizons derive from `ranked_at` as readable
    /// spans; no raw fractional epoch timestamp is printed, and unconfigured
    /// params, restrictions, and guidance lines are omitted, not fabricated.
    #[test]
    fn render_derives_ages_and_horizons_from_the_advice_clock() {
        let text = render(&json!({
            "ranked_at": 10000.0,
            "providers": [{
                "provider": "p", "harness": "codex", "models": [
                    {"model": "seen", "profiles": ["code"],
                     "quota": {"status": "available", "evidence": "fresh",
                               "newest_observed_at": 9955.0}},
                    {"model": "spent", "profiles": [],
                     "quota": {"status": "exhausted", "evidence": "fresh",
                               "newest_observed_at": 9990.0,
                               "exhausted_until": 17200.0}},
                ],
            }]
        }))
        .unwrap();
        assert!(
            text.contains("- seen: quota available, evidence fresh, sample age 45s"),
            "{text}"
        );
        assert!(text.contains("resets in 2h"), "{text}");
        assert!(text.contains("sample age 10s"), "{text}");
        assert!(!text.contains("9955"), "{text}");
        assert!(!text.contains("17200"), "{text}");
        assert!(text.contains("profiles: code"), "{text}");
        assert!(text.contains("profiles: none"), "{text}");
        assert!(!text.contains("params:"), "{text}");
        assert!(!text.contains("restrictions:"), "{text}");
        assert!(text.ends_with('\n'), "{text}");
    }
}
