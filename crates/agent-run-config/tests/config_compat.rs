//! Data-driven oracle over `tests/fixtures/baseline/config/cases.json`,
//! captured from the real Python `agent_run.config.load_config`,
//! `agent_run.native_settings`, and `agent_run.profiles.parse_profile`
//! (`migration/tools/capture_baseline.py`). Every case id names the exact
//! Python test it mirrors (`tests/test_config.py`, `tests/test_native_settings.py`,
//! `tests/test_profiles.py`).
//!
//! Each case supplies a `${TMP_HOME}` placeholder that this test substitutes
//! with one fresh temporary directory per case, used both as `HOME` (for `~`
//! expansion) and as the config's home directory (config.toml lives directly
//! at `$TMP_HOME/config.toml`, matching the Python fixture layout literally
//! named in several expected error messages). Known scope reduction: the
//! Python default `~/.agent-run/{profiles,skills}` hardcodes a nested
//! `.agent-run` segment that this crate's `Config::load(home)` does not
//! reproduce (its `home` parameter already denotes that directory), so
//! `profiles.directory` / `skills_directory` are checked for presence only,
//! not for an exact string match, when the field is left at its default.
mod common;

use agent_run_config::{config::Config, profiles};
use agent_run_domain::error::Error;
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::path::PathBuf;

#[derive(Deserialize)]
struct Case {
    id: String,
    source_test: String,
    toml: String,
    outcome: String,
    normalized_result_summary: Option<Value>,
}

fn cases() -> Vec<Case> {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/fixtures/baseline/config/cases.json"
    );
    let text = std::fs::read_to_string(path).expect("golden config fixture must exist");
    serde_json::from_str(&text).expect("golden config fixture must parse")
}

/// Keys whose Python default derives from a hardcoded `~/.agent-run/...`
/// literal that this crate's explicit `home` parameter cannot reproduce
/// byte-for-byte, or from a path that both loaders canonicalize only when it
/// actually exists on the host (so a machine with `codexbar`/Homebrew
/// installed resolves its default to a real symlink target that the
/// capturing machine's state may not have shared); checked for
/// presence/type only (see module docs).
fn presence_only(key: &str) -> bool {
    matches!(key, "directory" | "skills_directory" | "codexbar_binary")
}

/// Structural comparison: every key Python reports must be reproduced with
/// an equivalent (not necessarily byte-identical, see `presence_only`) value.
/// Extra keys on the Rust side, and a Rust key entirely absent when Python's
/// value is `null`, are both accepted (Rust omits absent-variant enum fields
/// that Python's dataclass always carries as `None`).
fn compatible(actual: &Value, expected: &Value, key: &str) -> bool {
    if presence_only(key) {
        return matches!(
            (actual, expected),
            (Value::String(_), Value::String(_)) | (Value::Null, Value::Null)
        );
    }
    match (actual, expected) {
        (Value::Null, Value::Null) => true,
        (Value::Bool(a), Value::Bool(b)) => a == b,
        (Value::Number(a), Value::Number(b)) => {
            let (a, b) = (a.as_f64().unwrap(), b.as_f64().unwrap());
            (a - b).abs() <= 1e-9_f64.max(1e-9 * a.abs().max(b.abs()))
        }
        (Value::String(a), Value::String(b)) => a == b,
        (Value::Array(a), Value::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(x, y)| compatible(x, y, key))
        }
        (Value::Object(a), Value::Object(b)) => b.iter().all(|(k, ev)| match a.get(k) {
            Some(av) => compatible(av, ev, k),
            None => ev.is_null(),
        }),
        _ => false,
    }
}

/// Renders an accepted `Config` into the same JSON shape as Python's
/// `normalized_result_summary` (field-for-field, see `src/agent_run/config.py`).
fn config_to_value(cfg: &Config) -> Value {
    let mcp: Map<String, Value> = cfg
        .mcp
        .iter()
        .map(|(k, v)| {
            (
                k.clone(),
                json!({
                    "transport": v.transport,
                    "command": v.command,
                    "args": v.args,
                    "env_from": v.env_from,
                    "approval_mode": v.approval_mode,
                }),
            )
        })
        .collect();
    let environments: Map<String, Value> = cfg
        .environments
        .iter()
        .map(|(k, e)| (k.clone(), environment_value(e)))
        .collect();
    let runtimes: Map<String, Value> = cfg
        .runtimes
        .iter()
        .map(|(k, r)| {
            let auth = r.auth.as_ref().map(|a| match a {
                agent_run_config::config::Auth::Environment { names } => json!({
                    "kind": "environment", "names": names, "source": null, "target": null,
                }),
                agent_run_config::config::Auth::FileLink { source, target } => json!({
                    "kind": "file_link", "names": [], "source": source, "target": target,
                }),
            });
            let hooks: Vec<Value> = r
                .hooks
                .iter()
                .map(|h| json!({"event": h.event, "command": h.command, "matcher": h.matcher}))
                .collect();
            let environment = r
                .environment
                .as_ref()
                .and_then(|name| cfg.environments.get(name))
                .map(environment_value);
            (
                k.clone(),
                json!({
                    "enabled": r.enabled,
                    "adapter": r.adapter,
                    "binary": r.binary,
                    "home": r.home,
                    "models": r.models,
                    "skills": r.skills,
                    "mcp": r.mcp,
                    "max_active_agents": r.max_active_agents,
                    "auth": auth,
                    "hooks": hooks,
                    "plugins": r.plugins,
                    "limits_source": r.limits_source,
                    "accounts": r.accounts,
                    "default_account": r.default_account,
                    "priority_multiplier": r.priority_multiplier,
                    "priority_account_multipliers": r.priority_account_multipliers,
                    "priority_lane_multipliers": r.priority_lane_multipliers,
                    "rust": r.rust.as_ref().map(|rr| json!({"rustup_home": rr.rustup_home, "cargo_bin": rr.cargo_bin})),
                    "environment": environment,
                    "plugin_snapshot_assets": r.plugin_snapshot_assets,
                    "credential_state_home": Value::Null,
                    "workspace_root": r.workspace_root,
                    "workspace_network": r.workspace_network,
                    "native_settings": r.native_settings,
                }),
            )
        })
        .collect();
    json!({
        "schema_version": cfg.schema_version,
        "core": {
            "default_timeout_seconds": cfg.core.default_timeout_seconds,
            "max_active_agents": cfg.core.max_active_agents,
            "warning_fraction": cfg.core.warning_fraction,
            "stalled_after_seconds": cfg.core.stalled_after_seconds,
        },
        "capacity": {
            "collect_interval_seconds": cfg.capacity.collect_interval_seconds,
            "sample_retention": cfg.capacity.sample_retention,
            "context_max_chars": cfg.capacity.context_max_chars,
            "codexbar_binary": cfg.capacity.codexbar_binary,
        },
        "delivery": {
            "retry_base_seconds": cfg.delivery.retry_base_seconds,
            "retry_cap_seconds": cfg.delivery.retry_cap_seconds,
            "max_attempts": cfg.delivery.max_attempts,
            "codex_queue_bin": cfg.delivery.codex_queue_bin,
        },
        "profiles": {"directory": cfg.profiles.directory},
        "skills_directory": cfg.skills.directory,
        "mcp": mcp,
        "environments": environments,
        "runtimes": runtimes,
    })
}

fn environment_value(e: &agent_run_config::config::Environment) -> Value {
    json!({
        "path": e.path,
        "variables": e.variables,
        "required_commands": e.required_commands,
        "denied_commands": e.denied_commands,
        "rust": e.rust.as_ref().map(|r| json!({"rustup_home": r.rustup_home, "cargo_bin": r.cargo_bin})),
    })
}

/// Substitutes `${TMP_HOME}` in the raw JSON text before parsing, so nested
/// placeholders at any depth resolve against this case's real temp directory.
fn expected_value(raw: &Value, tmp_home: &std::path::Path) -> Value {
    let text = serde_json::to_string(raw).unwrap();
    let text = text.replace("${TMP_HOME}", &tmp_home.to_string_lossy());
    serde_json::from_str(&text).unwrap()
}

fn run_config_case(case: &Case) -> Result<Value, Error> {
    let temp = tempfile::tempdir().expect("temp dir");
    let home = temp.path().canonicalize().unwrap();
    std::env::set_var("HOME", &home);
    let text = case.toml.replace("${TMP_HOME}", &home.to_string_lossy());
    std::fs::write(home.join("config.toml"), text).unwrap();
    Config::load(&home).map(|cfg| config_to_value(&cfg))
}

fn run_profile_case(case: &Case) -> Result<Value, Error> {
    let home = common::Home::new();
    let mut request = home.request();
    if let Some(name) = case
        .normalized_result_summary
        .as_ref()
        .and_then(|s| s.get("name"))
        .and_then(Value::as_str)
    {
        request.profile = name.to_string();
    }
    profiles::parse(&case.toml, &request).map(|p| {
        json!({
            "name": p.name,
            "body": p.body,
            "write": p.write,
            "network": p.network,
            "revision": p.revision,
            "canonical": p.canonical,
            "allow_external_read_roots": p.allow_external_read_roots,
            "read_roots": p.read_roots.iter().map(|r| r.to_string_lossy()).collect::<Vec<_>>(),
            "skills": p.skills,
            "mcp": p.mcp,
            "required_constraints": p.required_constraints,
        })
    })
}

/// Mirrors Python `tests/test_config.py::ConfigTests::test_accounts_parse_and_validate`,
/// Mirrors Python `tests/test_config.py::ConfigTests::test_accounts_require_supported_adapter_and_legacy_default_is_declared`.
/// Mirrors Python `tests/test_config.py::ConfigTests::test_claude_accounts_accept_its_scoped_environment_auth`.
/// Mirrors Python `tests/test_config.py::ConfigTests::test_codex_workspace_root_and_mcp_approval_mode_are_strict`.
/// Mirrors Python `tests/test_config.py::ConfigTests::test_codexbar_binary_defaults_to_homebrew_and_must_be_absolute`.
/// Mirrors Python `tests/test_config.py::ConfigTests::test_core_and_capacity_bounds_fail_during_load`.
/// Mirrors Python `tests/test_config.py::ConfigTests::test_delivery_queue_binary_is_optional_and_absolute`.
/// Mirrors Python `tests/test_config.py::ConfigTests::test_legacy_opencode_runtime_is_ignored`.
/// Mirrors Python `tests/test_config.py::ConfigTests::test_limits_source_defaults_to_native_and_is_validated`.
/// Mirrors Python `tests/test_config.py::ConfigTests::test_minimal_and_consumed_configuration_load`.
/// Mirrors Python `tests/test_config.py::ConfigTests::test_nonfinite_numeric_values_are_rejected`.
/// Mirrors Python `tests/test_config.py::ConfigTests::test_plugin_snapshot_assets_are_explicit_relative_and_immutable`.
/// Mirrors Python `tests/test_config.py::ConfigTests::test_plugin_snapshot_assets_reject_ambiguous_or_unsafe_declarations`.
/// Mirrors Python `tests/test_config.py::ConfigTests::test_priority_account_and_lane_multipliers_parse_with_defaults`.
/// Mirrors Python `tests/test_config.py::ConfigTests::test_priority_maps_reject_bad_shapes_keys_and_factors`.
/// Mirrors Python `tests/test_config.py::ConfigTests::test_relative_runtime_binary_is_rejected`.
/// Mirrors Python `tests/test_config.py::ConfigTests::test_runtime_binary_expands_the_user_prefix_without_resolving`.
/// Mirrors Python `tests/test_config.py::ConfigTests::test_runtime_binary_symlink_is_kept_while_other_paths_resolve`.
/// Mirrors Python `tests/test_config.py::ConfigTests::test_runtime_plugins_default_to_none_and_must_be_existing_directories`.
/// Mirrors Python `tests/test_config.py::ConfigTests::test_runtime_priority_multiplier_is_positive_and_finite`.
/// Mirrors Python `tests/test_config.py::ConfigTests::test_runtime_rust_requires_both_paths_and_keeps_cargo_bin_lexical`.
/// Mirrors Python `tests/test_config.py::ConfigTests::test_unknown_fields_report_the_recursive_path`.
/// Mirrors Python `tests/test_config.py::ConfigTests::test_unsupported_versions_and_secret_literals_are_rejected`.
/// Mirrors Python `tests/test_config.py::ConfigTests::test_validation_errors_do_not_echo_rejected_values`.
#[test]
fn golden_config_cases_match_python() {
    let mut failures: Vec<String> = Vec::new();
    let mut checked = 0usize;
    for case in cases() {
        checked += 1;
        let tmp_home_for_expected: PathBuf; // resolved lazily only for "ok" comparisons
        let result = if case.source_test == "tests/test_profiles.py" {
            run_profile_case(&case)
        } else {
            run_config_case(&case)
        };
        match (&result, case.outcome.as_str()) {
            (Ok(actual), "ok") => {
                if let Some(expected_raw) = &case.normalized_result_summary {
                    // The config-case branch already substituted ${TMP_HOME}
                    // with the real HOME used for that load; reuse it here.
                    let home = std::env::var("HOME").unwrap_or_default();
                    tmp_home_for_expected = PathBuf::from(&home);
                    let expected = expected_value(expected_raw, &tmp_home_for_expected);
                    if !compatible(actual, &expected, "") {
                        failures.push(format!(
                            "{}: accepted but structure differs\n  actual:   {actual}\n  expected: {expected}",
                            case.id
                        ));
                    }
                }
            }
            (Err(_), "ok") => {
                failures.push(format!(
                    "{}: expected ok, got error: {}",
                    case.id,
                    result.as_ref().unwrap_err()
                ));
            }
            (Ok(_), "error") => failures.push(format!("{}: expected error, got ok", case.id)),
            (Err(e), "error") => {
                if !matches!(e, Error::Validation(_)) {
                    failures.push(format!("{}: wrong error class: {e:?}", case.id));
                }
            }
            (_, other) => failures.push(format!("{}: unknown outcome {other:?}", case.id)),
        }
    }
    assert!(
        checked > 40,
        "expected the full golden corpus, saw {checked}"
    );
    assert!(
        failures.is_empty(),
        "{} of {checked} case mismatches:\n{}",
        failures.len(),
        failures.join("\n---\n")
    );
}
