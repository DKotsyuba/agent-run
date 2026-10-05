//! Guard qualification for native cache publication and global launches.
//!
//! Every test here drives the real qualification body — a Seatbelt sentinel
//! under the store-wide publish lock plus the harness's own boundary — with
//! no mock guard. Isolated fixture binaries stand in for the harness only
//! where a native selector would run the real one; the sentinel probes
//! themselves are real.
#![cfg(feature = "test-fixtures")]

use agent_run_adapters::{LaunchPlan, provider::ProviderLaunchPlan};
use agent_run_config::{config::Runtime, profiles::Profile};
use agent_run_core::supervisor;
use agent_run_domain::{
    catalog::HarnessId,
    domain::{AgentId, StartRequest},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs as stdfs,
    path::{Path, PathBuf},
};

/// One V1-shaped Codex runtime, exactly as the established grant tests build.
fn runtime(root: &Path) -> Runtime {
    serde_json::from_value(serde_json::json!({
        "enabled": true, "adapter": "codex", "binary": "/usr/bin/true",
        "home": root.join("runtime"),
        "models": ["gpt-5.6-sol", "gpt-6-astra"],
    }))
    .unwrap()
}

/// One write or read-only request pinned to `workdir`.
fn request(workdir: &Path, write: bool) -> StartRequest {
    serde_json::from_value(serde_json::json!({
        "runtime":"codex","model":"gpt-5.6-sol","profile":"review",
        "task":"fixture","workdir":workdir,"write":write,"fast":false,
    }))
    .unwrap()
}

/// One frozen role profile with caller-selected write authority.
fn profile_writing(write: bool) -> Profile {
    Profile {
        name: "review".into(),
        body: "Review the fixture.".into(),
        write,
        network: false,
        revision: "fixture".into(),
        canonical: false,
        allow_external_read_roots: true,
        read_roots: vec![],
        skills: vec![],
        mcp: vec![],
        mcp_tools: Default::default(),
        required_constraints: BTreeSet::new(),
    }
}

/// One valid read-only resolved-role payload, with its self-consistent
/// canonical config revision.
fn role_payload() -> serde_json::Value {
    let mut payload = serde_json::json!({
        "role_name": "review",
        "role_revision": "fixture",
        "prompt": "Review the fixture.",
        "grants": {
            "write": false,
            "network": false,
            "allow_external_read_roots": false,
            "read_roots": [],
        },
        "skills": [],
        "mcp": [],
        "required_constraints": [],
        "auth": {"mode": "global", "reference": null},
    });
    let revision = agent_run_domain::canonical::sha256_hex(&payload, true);
    payload["config_revision"] = serde_json::json!(revision);
    payload
}

/// One minimal frozen launch plan whose only qualification inputs are the
/// harness binary and its environment.
fn plan(binary: &Path) -> ProviderLaunchPlan {
    let role =
        agent_run_config::role_plan::ResolvedRolePlan::from_payload(&role_payload()).unwrap();
    let mut runtime = runtime(Path::new("/tmp"));
    runtime.binary = binary.to_path_buf();
    ProviderLaunchPlan {
        launch: LaunchPlan {
            binary: binary.to_path_buf(),
            args: vec!["app-server".into()],
            cwd: std::env::temp_dir(),
            environment: BTreeMap::new(),
            initial_input: None,
        },
        native_model: "gpt-5.6-sol".into(),
        role,
        runtime,
        profile: profile_writing(false),
    }
}

/// Creates the empty shared store root below one app home.
fn store_root(app_home: &Path) -> PathBuf {
    let root = app_home.join("shared-assets").join("v1");
    stdfs::create_dir_all(&root).expect("store root");
    root
}

/// A Codex grant keeps the shared store out of its writable roots: a store
/// inside the admitted workspace is refused, one beside it is admitted. This
/// is the root check that prevents publishing shared data a native run could
/// then legitimately write.
#[test]
fn codex_grant_refuses_a_store_inside_its_writable_root() {
    let temporary = tempfile::tempdir().unwrap();
    let workdir = temporary.path().join("work");
    stdfs::create_dir_all(&workdir).unwrap();
    let grant = agent_run_core::codex::Grant::new(
        &runtime(temporary.path()),
        &request(&workdir, true),
        &profile_writing(true),
        temporary.path(),
    )
    .unwrap();
    assert!(!grant.writable_roots.is_empty());

    // The store inside the writable workspace cannot be shared.
    let inside = store_root(&workdir);
    let error = grant.admits_shared_root(&inside).unwrap_err();
    assert!(
        error.to_string().contains("writable"),
        "the refusal names the overlap: {error}"
    );

    // A store under the effective temporary root is refused the same way:
    // native sandbox temp directories overlap it.
    let beside = store_root(temporary.path());
    let error = grant.admits_shared_root(&beside).unwrap_err();
    assert!(
        error.to_string().contains("writable"),
        "temporary overlap is refused too: {error}"
    );
}

/// Qualification runs the real sentinel probes: a harness whose guarded
/// metadata probe fails disqualifies the root, and a working one returns the
/// witness root that publication requires.
#[test]
fn qualification_proves_the_boundary_and_returns_the_witness() {
    let temporary = tempfile::tempdir().unwrap();
    let app_home = temporary.path().canonicalize().unwrap();
    let root = store_root(&app_home);

    // A harness that cannot even start under the guard disqualifies the root.
    let error =
        supervisor::qualify_shared_root(&app_home, &plan(Path::new("/usr/bin/false")), None)
            .unwrap_err();
    assert!(
        error.to_string().contains("guarded harness version probe"),
        "{error}"
    );

    // A working harness qualifies and returns exactly the store root.
    let witness =
        supervisor::qualify_shared_root(&app_home, &plan(Path::new("/usr/bin/true")), None)
            .expect("qualified");
    assert_eq!(witness, root);
    // The sentinel probes left nothing behind in the store.
    let leftovers: Vec<_> = stdfs::read_dir(&root)
        .unwrap()
        .flatten()
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with(".agent-run-probe")
        })
        .collect();
    assert!(leftovers.is_empty(), "probes clean up: {leftovers:?}");
}

/// Qualification with a Codex grant keeps the grant check: a store inside
/// the admitted workspace is refused before any publication, and a refused
/// qualification leaves no shared objects behind.
#[test]
fn codex_qualification_refuses_overlapping_store_before_publication() {
    let temporary = tempfile::tempdir().unwrap();
    let workdir = temporary.path().join("work");
    stdfs::create_dir_all(&workdir).unwrap();
    let app_home = workdir.canonicalize().unwrap();
    store_root(&app_home);

    let grant = agent_run_core::codex::Grant::new(
        &runtime(temporary.path()),
        &request(&workdir, true),
        &profile_writing(true),
        temporary.path(),
    )
    .unwrap();
    let error =
        supervisor::qualify_shared_root(&app_home, &plan(Path::new("/usr/bin/true")), Some(&grant))
            .unwrap_err();
    assert!(
        error.to_string().contains("writable"),
        "the grant overlap is the refusal: {error}"
    );
    // Nothing was published into the refused store: apart from the lock file
    // the qualification itself created, no object exists.
    let published: Vec<_> = stdfs::read_dir(app_home.join("shared-assets").join("v1"))
        .unwrap()
        .flatten()
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name != ".publish.lock")
        .collect();
    assert!(
        published.is_empty(),
        "no anchor, tree or blob exists: {published:?}"
    );
}

/// Publication requires the qualified witness root: any other root is
/// refused without touching the store, so an unqualified caller cannot
/// masquerade as a qualified one.
#[test]
fn consolidation_requires_the_qualified_witness_root() {
    let temporary = tempfile::tempdir().unwrap();
    let app_home = temporary.path().canonicalize().unwrap();
    let root = store_root(&app_home);
    let home = app_home.join("runs").join("ag-20260101-000000-0000000001");
    stdfs::create_dir_all(&home).expect("home");
    let mut store = agent_run_core::state::Store::initialize(&app_home).unwrap();
    let id: AgentId = "ag-20260101-000000-0000000001".parse().unwrap();
    let identity = fixture_identity(&app_home, &home);
    let account: agent_run_domain::AccountId = "acct-work".parse().unwrap();
    let foreign = app_home.join("elsewhere");
    stdfs::create_dir_all(&foreign).expect("foreign root");
    let error = agent_run_core::runtime_cache::consolidate(
        &mut store, &id, &identity, &account, &app_home, &home, &foreign,
    )
    .unwrap_err();
    assert!(error.to_string().contains("qualified"), "{error}");
    // The real witness root is accepted as the publication target.
    let witness =
        supervisor::qualify_shared_root(&app_home, &plan(Path::new("/usr/bin/true")), None)
            .expect("qualified");
    assert_eq!(witness, root);
    let report = agent_run_core::runtime_cache::consolidate(
        &mut store, &id, &identity, &account, &app_home, &home, &witness,
    )
    .expect("consolidated behind the witness");
    assert!(
        report.frozen + report.packed + report.skipped > 0,
        "{report:?}"
    );
}

/// One frozen Codex identity for a cache-only home with no managed roots.
fn fixture_identity(
    app_home: &Path,
    home: &Path,
) -> agent_run_core::service::ProviderLaunchIdentity {
    let config_text = format!(
        "schema_version = 2\n\
         [harnesses.codex]\n\
         binary = \"/usr/bin/true\"\n\
         home = \"{codex}\"\n\
         [harnesses.claude-code]\n\
         binary = \"/usr/bin/true\"\n\
         home = \"{claude}\"\n\
         [providers.codex-user]\n\
         harness = \"codex\"\n\
         connection = {{ kind = \"native\" }}\n\
         auth_family = \"openai\"\n\
         limits_source = \"none\"\n\
         [[providers.codex-user.models]]\n\
         id = \"fixture\"\n\
         native_model = \"fixture\"\n\
         [[providers.codex-user.bindings]]\n\
         label = \"work\"\n\
         account = \"acct-work\"\n",
        codex = app_home.join("codex").display(),
        claude = app_home.join("claude").display(),
    );
    let config =
        agent_run_config::provider_config::ProviderConfig::parse(&config_text, app_home).unwrap();
    // Seal a real index so the anchor can strictly verify the home.
    stdfs::write(home.join("config.toml"), "role = \"explore\"\n").unwrap();
    let digest = agent_run_platform::snapshot_tree::finalize_runtime_snapshots(
        home,
        "revision-native",
        &["config.toml".into()],
        &[],
    )
    .unwrap();
    agent_run_core::service::ProviderLaunchIdentity {
        provider_identity_version: 2,
        replay_request_sha256: "0".repeat(64),
        provider_request: serde_json::from_value(serde_json::json!({
            "provider":"codex-user","model":"fixture","profile":"review",
            "task":"fixture task","workdir":app_home,"account":"acct-work",
        }))
        .unwrap(),
        provider_config_sha256: config.snapshot().unwrap()["sha256"]
            .as_str()
            .unwrap()
            .to_owned(),
        provider_config: config,
        provider_config_snapshot: serde_json::json!({}),
        authority: agent_run_domain::catalog::ResolvedLaunchAuthority {
            provider: "codex-user".parse().unwrap(),
            harness: HarnessId::Codex,
            connection: agent_run_domain::ProviderConnection::Native,
            model: "fixture".into(),
            effort: None,
            profile: "review".into(),
            workdir: app_home.to_path_buf(),
            role_payload: role_payload(),
            assets_sha256: digest.parse().unwrap(),
            eligible_accounts: vec!["acct-work".parse().unwrap()],
        },
        runtime_home: Some(home.to_path_buf()),
        snapshot_sha256: Some(digest.as_str().into()),
    }
}
