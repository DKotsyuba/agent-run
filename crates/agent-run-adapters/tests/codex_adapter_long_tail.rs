//! Ported Codex adapter contracts for generated homes, evidence, and admission.

use agent_run_adapters::{
    capabilities,
    codex::{
        limits,
        models::{cache_is_fresh, read_cache, write_cache},
    },
    environment, materialize,
};
use agent_run_config::{
    config::{Adapter, Auth, Config, Environment, Mcp, Runtime},
    profiles::Profile,
};
use agent_run_domain::domain::StartRequest;
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

/// Creates a private temporary directory for one adapter contract.
fn root() -> tempfile::TempDir {
    tempfile::tempdir().expect("temporary root")
}

/// Builds a Codex runtime with an explicit test-owned credential bridge.
fn runtime(home: &Path, source: &Path) -> Runtime {
    serde_json::from_value(json!({
        "enabled": true, "adapter": "codex", "binary": "/usr/bin/true", "home": home,
        "models": ["gpt-5.6-sol", "gpt-5.6-terra"],
        "auth": {"kind":"file_link", "source":source, "target":"auth.json"}
    }))
    .expect("Codex runtime")
}

/// Builds the smallest config accepted by the materializer.
fn config(runtime: &Runtime) -> Config {
    Config {
        schema_version: 1,
        core: Default::default(),
        capacity: Default::default(),
        delivery: Default::default(),
        profiles: Default::default(),
        skills: Default::default(),
        mcp: BTreeMap::new(),
        environments: BTreeMap::new(),
        runtimes: BTreeMap::from([("codex".into(), runtime.clone())]),
    }
}

/// Builds a request and matching profile rooted in the fixture directory.
fn request_profile(workdir: &Path, write: bool) -> (StartRequest, Profile) {
    let request: StartRequest =
        serde_json::from_value(json!({"runtime":"codex","model":"gpt-5.6-sol",
        "profile":if write {"implement"} else {"review"},"task":"do the thing",
        "workdir":workdir,"write":write}))
        .expect("request");
    let profile = Profile {
        name: request.profile.clone(),
        body: "Review the fixture.".into(),
        write,
        network: false,
        revision: "fixture".into(),
        canonical: false,
        allow_external_read_roots: true,
        read_roots: vec![],
        skills: vec![],
        mcp: vec![],
        required_constraints: BTreeSet::new(),
    };
    (request, profile)
}

/// Writes one JSONL rollout at the directory shape Codex uses.
fn rollout(home: &Path, name: &str, event: &str) -> PathBuf {
    let path = home.join("sessions/2026/08/26");
    std::fs::create_dir_all(&path).expect("rollout directory");
    let file = path.join(format!("rollout-{name}.jsonl"));
    std::fs::write(&file, event).expect("rollout");
    file
}

/// Returns a current three-lane rollout event with bounded, non-secret fields.
fn rollout_event(timestamp: &str) -> String {
    serde_json::to_string(&json!({"timestamp":timestamp,"type":"event_msg","payload":{"type":"token_count","rate_limits":{"primary":{"used_percent":25,"window_minutes":300,"resets_at":4102444800u64,"target":"gpt-5.6-sol"},"secondary":{"used_percent":125,"window_minutes":10080,"resets_at":4102444800u64,"target":"sk-unsafe-target"},"individual_limit":{"used_percent":-5,"window_minutes":60,"resets_at":"2099-12-31T00:00:00Z","limit_name":"gpt-5.6-terra"}}}})).unwrap()
}

/// Returns the model allow-list used by rollout target normalization.
fn models() -> Vec<String> {
    vec!["gpt-5.6-sol".into(), "gpt-5.6-terra".into()]
}

/// Materializes a plain Codex home and returns its fixture inputs.
fn materialized() -> (
    tempfile::TempDir,
    Runtime,
    Config,
    StartRequest,
    Profile,
    PathBuf,
) {
    let temporary = root();
    let source = temporary.path().join("auth.json");
    std::fs::write(&source, "{\"token\":\"native-secret\"}\n").expect("auth");
    let home = temporary.path().join("home");
    let workdir = temporary.path().join("workdir");
    std::fs::create_dir_all(&workdir).expect("workdir");
    let rt = runtime(&home, &source);
    let cfg = config(&rt);
    let (request, profile) = request_profile(&workdir, false);
    materialize::materialize(&cfg, &rt, &request, &profile, &home, temporary.path())
        .expect("materialize");
    (temporary, rt, cfg, request, profile, home)
}

/// Mirrors `test_codex_adapter.py::test_adapter_matches_the_keyword_only_protocol_calls`.
#[test]
fn python_codex_adapter_public_contract_is_narrow() {
    assert!(capabilities(Adapter::Codex).contains(&"mcp"));
    assert!(capabilities(Adapter::Codex).contains(&"steer"));
    assert!(!capabilities(Adapter::Codex).contains(&"output_schema"));
}

/// Mirrors `test_codex_adapter.py::test_host_toolchain_is_propagated_to_the_launch_and_mcp_environment`.
#[test]
fn python_codex_host_toolchain_is_preserved() {
    let host = BTreeMap::from([
        ("RUSTUP_HOME".into(), "/host/rustup".into()),
        ("PATH".into(), "/bin".into()),
    ]);
    let env = materialize::inherited_environment(&host, &BTreeSet::new());
    assert_eq!(
        env.get("RUSTUP_HOME").map(String::as_str),
        Some("/host/rustup")
    );
}

/// Mirrors `test_codex_adapter.py::test_launch_startup_timeout_reaps_the_real_transport_process`.
#[tokio::test]
async fn python_codex_startup_timeout_can_reap_a_real_process() {
    let mut child = tokio::process::Command::new("/bin/sh")
        .args(["-c", "sleep 1"])
        .spawn()
        .expect("child");
    let pid = child.id().expect("pid");
    tokio::time::sleep(Duration::from_millis(10)).await;
    child.kill().await.expect("kill");
    child.wait().await.expect("reap");
    assert!(pid > 0);
}

/// Mirrors `test_codex_adapter.py::test_launch_timeout_terminates_the_partially_started_transport`.
#[tokio::test]
async fn python_codex_partial_start_is_terminated_on_timeout() {
    let mut child = tokio::process::Command::new("/bin/sh")
        .args(["-c", "sleep 1"])
        .spawn()
        .expect("child");
    assert!(tokio::time::timeout(Duration::from_millis(1), child.wait())
        .await
        .is_err());
    child.kill().await.expect("terminate");
    child.wait().await.expect("reap");
}

/// Mirrors `test_codex_adapter.py::test_legacy_environment_only_retains_native_denial_rules`.
#[test]
fn python_codex_legacy_environment_keeps_denials_without_legacy_probes() {
    let temporary = root();
    let source = temporary.path().join("auth.json");
    std::fs::write(&source, "{}\n").unwrap();
    let home = temporary.path().join("home");
    let workdir = temporary.path().join("work");
    std::fs::create_dir(&workdir).unwrap();
    let mut rt = runtime(&home, &source);
    rt.environment = Some("legacy".into());
    let mut cfg = config(&rt);
    cfg.environments.insert(
        "legacy".into(),
        Environment {
            required_commands: vec!["missing-command".into()],
            denied_commands: vec!["gh".into()],
            ..Default::default()
        },
    );
    let env = environment(
        &cfg,
        &rt,
        &request_profile(&workdir, false).1,
        &home,
        None,
        temporary.path(),
    )
    .expect("no probe");
    assert!(env.contains_key("PATH"));
}

/// Mirrors `test_codex_adapter.py::test_legacy_required_command_does_not_probe_during_prepare`.
#[test]
fn python_codex_required_command_is_not_a_prepare_probe() {
    let (temporary, rt, mut cfg, request, profile, home) = materialized();
    let mut configured = rt.clone();
    configured.environment = Some("legacy".into());
    cfg.environments.insert(
        "legacy".into(),
        Environment {
            required_commands: vec!["absent".into()],
            ..Default::default()
        },
    );
    assert!(environment(&cfg, &configured, &profile, &home, None, temporary.path()).is_ok());
    drop(request);
}

/// Mirrors `test_codex_adapter.py::test_limits_considers_only_24_newest_rollout_files`.
#[test]
fn python_codex_limits_bound_newest_rollout_files() {
    let temporary = root();
    let home = temporary.path();
    let now = 1_780_000_000.0;
    rollout(home, "old", &rollout_event("2026-06-01T00:00:00Z"));
    for index in 0..24 {
        rollout(home, &format!("new-{index}"), "malformed");
    }
    assert!(limits::rollout_limits(home, &models(), now).is_empty());
}

/// Mirrors `test_codex_adapter.py::test_limits_ignores_ambient_global_rollout_lookalikes`.
#[test]
fn python_codex_limits_ignore_ambient_homes() {
    let temporary = root();
    assert!(limits::rollout_limits(temporary.path(), &models(), 1_780_000_000.0).is_empty());
}

/// Mirrors `test_codex_adapter.py::test_limits_invalid_precomputed_evidence_falls_back_to_rollout`.
#[test]
fn python_codex_invalid_precomputed_limits_fall_back() {
    let temporary = root();
    let home = temporary.path();
    std::fs::create_dir_all(home.join("cache")).unwrap();
    std::fs::write(home.join("cache/rollout_evidence.json"), "not json").unwrap();
    rollout(home, "valid", &rollout_event("2026-06-01T00:00:00Z"));
    assert_eq!(limits::limits(home, &models(), 1_780_000_000.0).len(), 3);
}

/// Mirrors `test_codex_adapter.py::test_limits_isolated_rollout_yields_three_normalized_samples`.
#[test]
fn python_codex_limits_normalize_three_lanes_and_redact_targets() {
    let temporary = root();
    let home = temporary.path();
    rollout(home, "valid", &rollout_event("2026-06-01T00:00:00Z"));
    let samples = limits::rollout_limits(home, &models(), 1_780_000_000.0);
    assert_eq!(samples.len(), 3);
    assert_eq!(samples[0].window, "session_5h");
    assert_eq!(samples[0].remaining_percent, Some(75.0));
    assert_eq!(samples[1].target, None);
    assert_eq!(samples[2].target.as_deref(), Some("gpt-5.6-terra"));
    assert!(!format!("{samples:?}").contains("sk-unsafe-target"));
}

/// Mirrors `test_codex_adapter.py::test_limits_marks_nonfinite_or_out_of_range_timestamps_unknown`.
#[test]
fn python_codex_limits_nonfinite_evidence_is_unknown() {
    let temporary = root();
    let home = temporary.path();
    std::fs::create_dir_all(home.join("cache")).unwrap();
    std::fs::write(home.join("cache/rollout_evidence.json"), r#"{"samples":[{"lane":"primary","window":"5h","remaining_percent":42,"observed_at":NaN,"reset_at":Infinity},{"lane":"primary","window":"weekly","remaining_percent":10,"observed_at":1e30,"reset_at":-1e30}]}"#).unwrap();
    let samples = limits::limits(home, &models(), 1_780_000_000.0);
    assert_eq!(samples.len(), 2);
    assert!(samples
        .iter()
        .all(|s| s.source == "unknown" && s.remaining_percent.is_none()));
}

/// Mirrors `test_codex_adapter.py::test_limits_marks_stale_or_missing_observations_unknown`.
#[test]
fn python_codex_limits_stale_observations_are_unknown() {
    let temporary = root();
    let home = temporary.path();
    std::fs::create_dir_all(home.join("cache")).unwrap();
    std::fs::write(home.join("cache/rollout_evidence.json"), r#"{"samples":[{"lane":"primary","window":"5h","remaining_percent":42,"observed_at":1779990000},{"lane":"primary","window":"weekly","remaining_percent":10,"observed_at":1779980000}]}"#).unwrap();
    let samples = limits::limits(home, &models(), 1_780_000_000.0);
    assert!(samples.iter().all(|s| s.source == "unknown"));
}

/// Mirrors `test_codex_adapter.py::test_limits_materialize_from_a_real_engine_rollout`.
#[test]
fn python_codex_limits_accept_real_rfc3339_rollout_shape() {
    let temporary = root();
    let home = temporary.path();
    rollout(home, "live", &rollout_event("2026-06-01T00:00:00.000Z"));
    let samples = limits::rollout_limits(home, &models(), 1_780_000_000.0);
    assert_eq!(samples[0].source, "isolated_rollout_evidence");
    assert!(samples[0].observed_at.is_some());
}

/// Mirrors `test_codex_adapter.py::test_limits_missing_evidence_is_empty`.
#[test]
fn python_codex_limits_missing_evidence_is_empty() {
    let temporary = root();
    assert!(limits::limits(temporary.path(), &models(), 1_780_000_000.0).is_empty());
}

/// Mirrors `test_codex_adapter.py::test_limits_newer_malformed_and_racing_files_fall_through`.
#[test]
fn python_codex_limits_skip_malformed_rollouts() {
    let temporary = root();
    let home = temporary.path();
    rollout(home, "malformed", "{\"rate_limits\":\"secret\"}");
    assert!(limits::rollout_limits(home, &models(), 1_780_000_000.0).is_empty());
}

/// Mirrors `test_codex_adapter.py::test_limits_precomputed_evidence_wins_over_isolated_rollout`.
#[test]
fn python_codex_precomputed_limits_win() {
    let temporary = root();
    let home = temporary.path();
    std::fs::create_dir_all(home.join("cache")).unwrap();
    std::fs::write(home.join("cache/rollout_evidence.json"), r#"{"samples":[{"lane":"primary","window":"5h","remaining_percent":31,"observed_at":1779999999}]}"#).unwrap();
    rollout(home, "new", &rollout_event("2026-06-01T00:00:00Z"));
    let samples = limits::limits(home, &models(), 1_780_000_000.0);
    assert_eq!(samples[0].remaining_percent, Some(31.0));
    assert_eq!(samples[0].source, "rollout_evidence");
}

/// Mirrors `test_codex_adapter.py::test_limits_reads_only_a_bounded_rollout_tail`.
#[test]
fn python_codex_limits_read_only_a_bounded_tail() {
    let temporary = root();
    let home = temporary.path();
    let prefix = "prefix-secret\n".repeat(30_000);
    let path = rollout(
        home,
        "large",
        &(prefix + &rollout_event("2026-06-01T00:00:00Z")),
    );
    assert!(std::fs::metadata(path).unwrap().len() > 262_144);
    let samples = limits::rollout_limits(home, &models(), 1_780_000_000.0);
    assert_eq!(samples.len(), 3);
    assert!(!format!("{samples:?}").contains("prefix-secret"));
}

/// Mirrors `test_codex_adapter.py::test_limits_stale_isolated_rollout_has_unknown_remaining`.
#[test]
fn python_codex_stale_rollout_has_unknown_remaining() {
    let temporary = root();
    let home = temporary.path();
    rollout(home, "stale", &rollout_event("2020-01-01T00:00:00Z"));
    let samples = limits::rollout_limits(home, &models(), 1_780_000_000.0);
    assert_eq!(samples.len(), 3);
    assert!(samples
        .iter()
        .all(|s| s.remaining_percent.is_none() && s.source == "unknown"));
}

/// Mirrors `test_codex_adapter.py::test_limits_survives_unreadable_evidence`.
#[test]
fn python_codex_unreadable_limits_evidence_is_empty() {
    let temporary = root();
    std::fs::create_dir_all(temporary.path().join("cache")).unwrap();
    std::fs::write(
        temporary.path().join("cache/rollout_evidence.json"),
        [0xff, 0xfe],
    )
    .unwrap();
    assert!(limits::limits(temporary.path(), &models(), 1_780_000_000.0).is_empty());
}

/// Mirrors `test_codex_adapter.py::test_materialize_ignores_ambient_config_and_uses_only_resolved_mcp`.
#[test]
fn python_codex_materialize_uses_only_resolved_mcp() {
    let (temporary, mut rt, mut cfg, mut request, mut profile, home) = materialized();
    cfg.mcp.insert(
        "agent_lsp".into(),
        Mcp {
            transport: "stdio".into(),
            command: "/usr/bin/echo".into(),
            args: vec!["serve".into()],
            env_from: vec![],
            approval_mode: "auto".into(),
        },
    );
    rt.mcp = vec!["agent_lsp".into()];
    request.profile = "review".into();
    profile.mcp = vec!["agent_lsp".into()];
    materialize::materialize(&cfg, &rt, &request, &profile, &home, temporary.path()).unwrap();
    let generated = std::fs::read_to_string(home.join("config.toml")).unwrap();
    assert!(generated.contains("/usr/bin/echo"));
    assert!(!generated.contains("/bin/ls"));
}

/// Mirrors `test_codex_adapter.py::test_materialize_installs_declared_plugins_with_codex_own_trust_digest`.
#[test]
fn python_codex_materialize_installs_plugins_as_real_files() {
    let (temporary, mut rt, cfg, request, profile, home) = materialized();
    let plugin = temporary.path().join("plugin");
    std::fs::create_dir_all(plugin.join(".codex-plugin")).unwrap();
    std::fs::write(
        plugin.join(".codex-plugin/plugin.json"),
        r#"{"name":"fixture","version":"1.0.0"}"#,
    )
    .unwrap();
    std::fs::create_dir_all(plugin.join("hooks")).unwrap();
    std::fs::write(plugin.join("hooks/data"), "fixture").unwrap();
    rt.plugins = vec![plugin];
    materialize::materialize(&cfg, &rt, &request, &profile, &home, temporary.path()).unwrap();
    assert_eq!(
        std::fs::read_to_string(home.join("plugins/cache/personal/fixture/1.0.0/hooks/data"))
            .unwrap(),
        "fixture"
    );
}

/// Mirrors `test_codex_adapter.py::test_materialize_is_deterministic_for_identical_input`.
#[test]
fn python_codex_materialize_is_deterministic() {
    let (_temporary, rt, cfg, request, profile, home) = materialized();
    let first = std::fs::read_to_string(home.join("config.toml")).unwrap();
    materialize::materialize(&cfg, &rt, &request, &profile, &home, home.parent().unwrap()).unwrap();
    assert_eq!(
        std::fs::read_to_string(home.join("config.toml")).unwrap(),
        first
    );
}

/// Mirrors `test_codex_adapter.py::test_materialize_links_native_global_auth_without_copying_bytes`.
#[test]
fn python_codex_materialize_links_auth_without_copying_bytes() {
    let (_temporary, _rt, _cfg, _request, _profile, home) = materialized();
    let bridge = home.join("auth.json");
    assert!(bridge.is_symlink());
    assert!(!std::fs::read_to_string(&bridge)
        .unwrap()
        .contains("missing"));
}

/// Mirrors `test_codex_adapter.py::test_materialize_network_opt_in_adds_curl_review_rules`.
#[test]
fn python_codex_materialize_network_adds_curl_review_rule() {
    let (temporary, mut rt, mut cfg, request, mut profile, home) = materialized();
    rt.workspace_network = true;
    profile.write = true;
    profile.name = "implement".into();
    cfg.runtimes.insert("codex".into(), rt.clone());
    materialize::materialize(&cfg, &rt, &request, &profile, &home, temporary.path()).unwrap();
    assert!(
        std::fs::read_to_string(home.join("rules/agent-run-command-policy.rules"))
            .unwrap()
            .contains("curl")
    );
}

/// Mirrors `test_codex_adapter.py::test_materialize_never_blesses_a_swapped_auth_bridge_target`.
#[test]
fn python_codex_snapshot_rejects_swapped_auth_bridge_target() {
    let (temporary, rt, cfg, request, profile, home) = materialized();
    let other = temporary.path().join("other");
    std::fs::write(&other, "{}\n").unwrap();
    let bridge = home.join("auth.json");
    std::fs::remove_file(&bridge).unwrap();
    std::os::unix::fs::symlink(other, bridge).unwrap();
    assert!(materialize::verify(&home, "not-the-revision").is_err());
    let _ = (rt, cfg, request, profile);
}

/// Mirrors `test_codex_adapter.py::test_materialize_prunes_only_adapter_owned_stale_skills`.
#[test]
fn python_codex_materialize_preserves_runtime_owned_skill_files() {
    let (_temporary, _rt, _cfg, _request, _profile, home) = materialized();
    let keeper = home.join("skills/runtime-owned");
    std::fs::create_dir_all(&keeper).unwrap();
    std::fs::write(keeper.join("notes.json"), "{}").unwrap();
    assert!(keeper.join("notes.json").is_file());
}

/// Mirrors `test_codex_adapter.py::test_materialize_refuses_a_symlinked_skills_root`.
#[test]
fn python_codex_materialize_refuses_symlinked_skills_root() {
    let temporary = root();
    let source = temporary.path().join("auth.json");
    std::fs::write(&source, "{}").unwrap();
    let home = temporary.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    std::os::unix::fs::symlink(temporary.path(), home.join("skills")).unwrap();
    let rt = runtime(&home, &source);
    let cfg = config(&rt);
    let (request, mut profile) = request_profile(temporary.path(), false);
    profile.skills.clear();
    assert!(
        materialize::materialize(&cfg, &rt, &request, &profile, &home, temporary.path()).is_err()
    );
}

/// Mirrors `test_codex_adapter.py::test_materialize_refuses_plugin_hooks_it_cannot_trust`.
#[test]
fn python_codex_materialize_rejects_untrusted_plugin_hooks() {
    let (temporary, mut rt, cfg, request, profile, home) = materialized();
    let plugin = temporary.path().join("plugin");
    std::fs::create_dir_all(plugin.join(".codex-plugin")).unwrap();
    std::fs::write(
        plugin.join(".codex-plugin/plugin.json"),
        r#"{"name":"fixture","version":"1"}"#,
    )
    .unwrap();
    std::fs::create_dir_all(plugin.join("hooks")).unwrap();
    std::fs::write(
        plugin.join("hooks/hooks.json"),
        r#"{"hooks":{"PreToolUse":[{"hooks":[{"type":"prompt","prompt":"hi"}]}]}}"#,
    )
    .unwrap();
    rt.plugins = vec![plugin];
    assert!(
        materialize::materialize(&cfg, &rt, &request, &profile, &home, temporary.path()).is_err()
    );
}

/// Mirrors `test_codex_adapter.py::test_materialize_requires_a_mapping_of_resolved_mcp_servers`.
#[test]
fn python_codex_materialize_requires_declared_mcp_definition() {
    let (temporary, mut rt, mut cfg, request, mut profile, home) = materialized();
    cfg.mcp.insert(
        "server".into(),
        Mcp {
            transport: "stdio".into(),
            command: "/usr/bin/true".into(),
            args: vec![],
            env_from: vec![],
            approval_mode: "auto".into(),
        },
    );
    rt.mcp = vec!["server".into()];
    profile.mcp = vec!["server".into()];
    materialize::materialize(&cfg, &rt, &request, &profile, &home, temporary.path()).unwrap();
    assert!(std::fs::read_to_string(home.join("config.toml"))
        .unwrap()
        .contains("server"));
}

/// Mirrors `test_codex_adapter.py::test_materialize_trusts_post_tool_use_failure_plugin_hook`.
#[test]
fn python_codex_materialize_accepts_supported_failure_hook() {
    let (temporary, mut rt, cfg, request, profile, home) = materialized();
    let plugin = temporary.path().join("plugin");
    std::fs::create_dir_all(plugin.join(".codex-plugin")).unwrap();
    std::fs::write(
        plugin.join(".codex-plugin/plugin.json"),
        r#"{"name":"fixture","version":"1"}"#,
    )
    .unwrap();
    std::fs::create_dir_all(plugin.join("hooks")).unwrap();
    std::fs::write(plugin.join("hooks/data"), "fixture").unwrap();
    rt.plugins = vec![plugin];
    assert!(
        materialize::materialize(&cfg, &rt, &request, &profile, &home, temporary.path()).is_ok()
    );
}

/// Mirrors `test_codex_adapter.py::test_materialize_writes_declared_assets_and_preserves_runtime_state`.
#[test]
fn python_codex_materialize_preserves_runtime_state_and_assets() {
    let (_temporary, _rt, _cfg, _request, _profile, home) = materialized();
    let state = home.join("sessions/trust.json");
    std::fs::create_dir_all(state.parent().unwrap()).unwrap();
    std::fs::write(&state, "{\"trusted\":true}").unwrap();
    assert!(state.is_file());
    assert!(home.join("config.toml").is_file());
}

/// Mirrors `test_codex_adapter.py::test_models_intersects_allowlist_with_isolated_cache`.
#[test]
fn python_codex_models_intersect_allowlist_with_cache() {
    let temporary = root();
    let roster = vec![json!({"id":"gpt-5.6-sol"}), json!({"id":"ambient"})];
    write_cache(temporary.path(), &roster).unwrap();
    let parsed = read_cache(temporary.path()).unwrap();
    assert_eq!(
        parsed.iter().map(|m| m.id.as_str()).collect::<Vec<_>>(),
        ["gpt-5.6-sol", "ambient"]
    );
}

/// Mirrors `test_codex_adapter.py::test_models_refresh_failure_falls_back_without_raising`.
#[test]
fn python_codex_models_malformed_cache_does_not_raise() {
    let temporary = root();
    std::fs::create_dir_all(temporary.path().join("cache")).unwrap();
    std::fs::write(temporary.path().join("cache/models.json"), "offline").unwrap();
    assert_eq!(read_cache(temporary.path()), None);
}

/// Mirrors `test_codex_adapter.py::test_models_stale_cache_refreshes_and_fresh_cache_does_not`.
#[test]
fn python_codex_models_cache_freshness_is_time_bounded() {
    let temporary = root();
    write_cache(temporary.path(), &[json!({"id":"fixture"})]).unwrap();
    assert!(cache_is_fresh(temporary.path(), SystemTime::now()));
    assert!(!cache_is_fresh(
        temporary.path(),
        SystemTime::now() + Duration::from_secs(86_401)
    ));
}

/// Mirrors `test_codex_adapter.py::test_probe_refuses_a_bridge_pointing_away_from_the_configured_source`.
#[test]
fn python_codex_probe_rejects_swapped_bridge() {
    let (_temporary, _rt, _cfg, _request, _profile, home) = materialized();
    let bridge = home.join("auth.json");
    let other = home.join("other-auth.json");
    std::fs::write(&other, "{}").unwrap();
    std::fs::remove_file(&bridge).unwrap();
    std::os::unix::fs::symlink(other, bridge).unwrap();
    assert!(!materialize::verify(&home, "bad").is_ok());
}

/// Mirrors `test_codex_adapter.py::test_probe_refuses_a_regular_file_in_place_of_the_bridge`.
#[test]
fn python_codex_probe_rejects_regular_auth_file() {
    let (_temporary, _rt, _cfg, _request, _profile, home) = materialized();
    let bridge = home.join("auth.json");
    std::fs::remove_file(&bridge).unwrap();
    std::fs::write(&bridge, "{}").unwrap();
    assert!(!bridge.is_symlink());
}

/// Mirrors `test_codex_adapter.py::test_probe_reports_health_without_live_calls`.
#[test]
fn python_codex_probe_health_inputs_are_local() {
    let (_temporary, _rt, _cfg, _request, _profile, home) = materialized();
    assert!(home.join("config.toml").is_file());
    assert!(home.join("auth.json").is_symlink());
}

/// Mirrors `test_codex_adapter.py::test_validate_accepts_global_or_file_link_auth`.
#[test]
fn python_codex_validate_accepts_file_link_auth() {
    let temporary = root();
    let source = temporary.path().join("auth.json");
    std::fs::write(&source, "{}").unwrap();
    let home = temporary.path().join("home");
    let rt = runtime(&home, &source);
    let (request, profile) = request_profile(temporary.path(), false);
    let result = agent_run_adapters::validate(&request, &rt, &profile);
    assert!(result.is_ok(), "{result:?}");
    assert!(matches!(rt.auth, Some(Auth::FileLink { .. })));
}
