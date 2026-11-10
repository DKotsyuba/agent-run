//! Native owner admission fences shared by the existing Codex and Claude event loops.
use crate::{Result, persist::Persist, profiles::Profile, state::Record};
use agent_run_adapters::{
    io::Process,
    native_owner::{self, Call},
};
use agent_run_domain::domain::{Constraint, Status};
use agent_run_platform::process;
use std::collections::BTreeMap;

/// Strip retired build qualifiers from every launch and remove owner settings for one-shot, restricted or unsupported launches.
pub fn configure(
    environment: &mut BTreeMap<String, String>,
    record: &Record,
    role: &Profile,
    supported: bool,
) {
    for key in native_owner::RETIRED_SETTINGS {
        environment.remove(key);
    }
    if !supported
        || !record.request.explicit_finish
        || role.research_tools_only()
        || role
            .required_constraints
            .contains(&Constraint::UnixIpcIsolation)
        || role
            .required_constraints
            .contains(&Constraint::McpIpcIsolation)
    {
        for key in native_owner::SETTINGS {
            environment.remove(key);
        }
        environment.remove(native_owner::LOCATOR);
    }
}
/// Refuse before effect unless the same owned child/session and original logical admission are still live and positively idle.
/// Storage/identity failures are unavailable evidence, never permission to wake an unknown or terminal actor.
pub fn refusal(
    process: &Process,
    store: &Persist,
    record: &Record,
    call: &Call,
    session: Option<&str>,
    idle: bool,
    closing: bool,
) -> Result<Option<&'static str>> {
    let Some(native) = process.owner.leader.as_ref() else {
        return Ok(Some("unavailable"));
    };
    if native.pid as u32 != call.request.native_pid
        || native.birth.floor() as u64 != call.request.native_start_time
    {
        return Ok(Some("actor_mismatch"));
    }
    refusal_common(
        process,
        store,
        record,
        session,
        &call.request.thread_id,
        call.deadline,
        idle,
        closing,
    )
}

#[allow(clippy::too_many_arguments)]
fn refusal_common(
    process: &Process,
    store: &Persist,
    record: &Record,
    session: Option<&str>,
    target_session: &str,
    deadline: tokio::time::Instant,
    idle: bool,
    closing: bool,
) -> Result<Option<&'static str>> {
    if tokio::time::Instant::now() >= deadline {
        return Ok(Some("unavailable"));
    }
    let Some(native) = process.owner.leader.as_ref() else {
        return Ok(Some("unavailable"));
    };
    if session != Some(target_session) {
        return Ok(Some("actor_mismatch"));
    }
    if process::observe(Some(native.pid), Some(&native.token), Some(native.birth))
        != process::ProcessState::Alive
        || process.input.is_none()
    {
        return Ok(Some("unavailable"));
    }
    let actual = process::inspect(native.pid)?;
    if actual.ppid != std::process::id() as i32
        || actual.group != native.group
        || actual.token != native.token
        || actual.birth != native.birth
    {
        return Ok(Some("actor_mismatch"));
    }
    let current = store.get(&record.id)?;
    if current.status != Status::Running
        || closing
        || store.worker_finish_intent(&record.id, false)?.is_some()
    {
        return Ok(Some("finished"));
    }
    if serde_json::to_value(&current.request)? != serde_json::to_value(&record.request)?
        || current.sequence != record.sequence
        || current.identity != record.identity
    {
        return Ok(Some("actor_mismatch"));
    }
    let owned:bool=store.conn.query_row("SELECT EXISTS(SELECT 1 FROM attempts WHERE agent_id=? AND ownership_active=1 AND finished_at IS NULL AND process_identity=? AND process_birth_time=?)",rusqlite::params![record.id.as_str(),native.token,native.birth],|row|row.get(0))?;
    if !owned {
        return Ok(Some("actor_mismatch"));
    }
    if store.cancel_pending(&record.id)? {
        return Ok(Some("cancelled"));
    }
    if !idle {
        return Ok(Some("not_idle"));
    }
    let Some(lifecycle) = store.worker_lifecycle_view(&record.id)? else {
        return Ok(Some("unavailable"));
    };
    if lifecycle["phase"] != "idle" {
        return Ok(Some("not_idle"));
    }
    Ok(None)
}

/// Extend the shared owner fence with the complete frozen managed destination,
/// qualified launch and a fresh full kernel incarnation. No peer JSON supplies
/// a route or build grant.
#[allow(clippy::too_many_arguments)]
pub(crate) fn refusal_v2(
    process: &Process,
    store: &Persist,
    record: &Record,
    request: &agent_run_domain::owner_v2::RequestV2,
    deadline: tokio::time::Instant,
    channel: &std::path::Path,
    session: Option<&str>,
    idle: bool,
    closing: bool,
) -> Result<Option<&'static str>> {
    if request.validate().is_err() {
        return Ok(Some("invalid_request"));
    }
    if let Some(reason) = refusal_common(
        process,
        store,
        record,
        session,
        &request.thread_id,
        deadline,
        idle,
        closing,
    )? {
        return Ok(Some(reason));
    }
    let binding = &request.outbox_intent.target_binding;
    let attempt = &binding.attempt;
    let expected = &request.outbox_intent.expected_incarnation;
    if attempt.execution_id != record.id
        || attempt.lineage_root_id != record.root_agent_id
        || attempt.sequence != record.sequence
        || store.bound_attempt() != Some(attempt.attempt_id.as_str())
        || session != Some(binding.actor.native_id.as_str())
        || binding.actor.role != "root"
        || agent_run_store::store_identity::read(&store.conn)? != attempt.run_store_id
    {
        return Ok(Some("actor_mismatch"));
    }
    let frozen = crate::service::ProviderLaunchIdentity::read(record)?;
    let (live, _) = agent_run_config::provider_config::ProviderConfig::load(store.home())?;
    let Some(current) = live.managed_attempt_binding.as_ref().filter(|m| m.enabled) else {
        return Ok(Some("unavailable"));
    };
    let Some(original) = frozen
        .provider_config
        .managed_attempt_binding
        .as_ref()
        .filter(|m| m.enabled)
    else {
        return Ok(Some("unavailable"));
    };
    let (Some(current_route), Some(original_route)) = (&current.transport, &original.transport)
    else {
        return Ok(Some("unavailable"));
    };
    if serde_json::to_value(current_route)? != serde_json::to_value(original_route)?
        || channel != original_route.bridge_socket
        || channel.canonicalize().ok().as_ref() != Some(&original_route.bridge_socket)
        || current.host_id != original.host_id
        || current.role_name != original.role_name
        || expected.host_id != original.host_id
        || binding.run_route != original_route.run_route
        || binding.bridge_route != original_route.bridge_route
        || binding.pool_store_id != original_route.pool_route.component_store_id
    {
        return Ok(Some("actor_mismatch"));
    }
    let Some(leader) = process.owner.leader.as_ref() else {
        return Ok(Some("unavailable"));
    };
    let kernel = process::kernel_incarnation(leader)?;
    if kernel.boot_id != expected.boot_id
        || kernel.pid_namespace_inode != expected.pid_namespace_inode
        || kernel.uid != expected.uid
        || kernel.pid != expected.pid
        || kernel.start_ticks != expected.start_ticks
    {
        return Ok(Some("actor_mismatch"));
    }
    let qualified: bool = store.conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM attempt_launches l JOIN attempts t ON t.id=l.attempt_id JOIN attempt_role_qualifications q ON q.launch_id=l.launch_id AND q.attempt_id=t.id WHERE l.execution_id=? AND l.sequence=? AND l.attempt_id=? AND l.launch_id=? AND l.state='bound' AND t.ownership_active=1 AND t.finished_at IS NULL AND l.boot_id=? AND l.pid_namespace_inode=? AND l.uid=? AND l.pid=? AND l.start_ticks=? AND q.boot_id=l.boot_id AND q.pid_namespace_inode=l.pid_namespace_inode AND q.uid=l.uid AND q.pid=l.pid AND q.start_ticks=l.start_ticks)",
        rusqlite::params![record.id.as_str(),attempt.sequence,attempt.attempt_id,attempt.launch_id,expected.boot_id,expected.pid_namespace_inode,expected.uid,expected.pid,expected.start_ticks],|row|row.get(0),
    )?;
    if !qualified {
        return Ok(Some("attempt_not_ready"));
    }
    let receipt: bool = store.conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM (SELECT pool_route_json,receipt_json FROM attempt_current_receipts WHERE json_extract(receipt_json,'$.purpose')='egress' AND json_extract(receipt_json,'$.attempt.attempt_id')=? AND json_extract(receipt_json,'$.attempt.launch_id')=? ORDER BY rowid DESC LIMIT 1) WHERE pool_route_json=? AND json_extract(receipt_json,'$.attempt.execution_id')=? AND json_extract(receipt_json,'$.actor.address')=? AND json_extract(receipt_json,'$.actor.role')='root' AND json_extract(receipt_json,'$.actor.native_id')=? AND json_extract(receipt_json,'$.actor.epoch')=? AND json_extract(receipt_json,'$.incarnation.boot_id')=? AND json_extract(receipt_json,'$.incarnation.pid_namespace_inode')=? AND json_extract(receipt_json,'$.incarnation.uid')=? AND json_extract(receipt_json,'$.incarnation.pid')=? AND json_extract(receipt_json,'$.incarnation.start_ticks')=?)",
        rusqlite::params![attempt.attempt_id,attempt.launch_id,serde_json::to_string(&original_route.pool_route)?,record.id.as_str(),binding.actor.address,binding.actor.native_id,binding.actor.epoch,expected.boot_id,expected.pid_namespace_inode,expected.uid,expected.pid,expected.start_ticks],|row|row.get(0),
    )?;
    if !receipt {
        return Ok(Some("attempt_not_ready"));
    }
    Ok(None)
}

/// A reservation token is a single-lane permission to invoke once; everything
/// else is an already durable result or a conservative unknown.
pub(crate) enum PreparedV2 {
    Invoke(String),
    Reply(agent_run_domain::owner_v2::ReplyV2),
}

fn effect_reply(
    request: &agent_run_domain::owner_v2::RequestV2,
    view: &agent_run_store::owner_v2::EffectView,
) -> agent_run_domain::owner_v2::ReplyV2 {
    use agent_run_adapters::native_owner_v2::reply;
    match view.state.as_str() {
        "accepted" => reply(
            request,
            "accepted",
            view.protocol.as_deref(),
            view.native_id.as_deref(),
            None,
        ),
        "known_unsent" => reply(request, "known_unsent", None, None, view.reason.as_deref()),
        "aborted" => reply(request, "known_unsent", None, None, Some("conflict")),
        _ => reply(request, "unknown", None, None, Some("native_uncertain")),
    }
}

/// Read the original durable effect, never a caller-supplied outcome. A CAS
/// conflict can mean an abort or another dispatch already won the writer.
fn lookup_effect(
    store: &Persist,
    request: &agent_run_domain::owner_v2::RequestV2,
) -> Result<agent_run_store::owner_v2::EffectView> {
    let outbox = &request.outbox_intent;
    let route = agent_run_store::owner_v2::EffectRoute {
        run_route: outbox.target_binding.run_route.clone(),
        bridge_route: outbox.bridge_route.clone(),
    };
    store.owner_effect_lookup(
        &outbox.target_binding.attempt.run_store_id,
        &outbox.producer_scope,
        &outbox.effect_id,
        &request.effect_digest,
        &route,
    )
}

fn conflict_reply(
    store: &Persist,
    request: &agent_run_domain::owner_v2::RequestV2,
) -> agent_run_domain::owner_v2::ReplyV2 {
    use agent_run_adapters::native_owner_v2::reply;
    match lookup_effect(store, request) {
        Ok(view) if view.state != "absent" => effect_reply(request, &view),
        Err(agent_run_domain::Error::Conflict) | Ok(_) => {
            reply(request, "known_unsent", None, None, Some("conflict"))
        }
        Err(_) => reply(request, "unknown", None, None, Some("native_uncertain")),
    }
}

/// Reserve, check the destination, commit invoking, then recheck on the same
/// native input lane. A same-writer abort winning before invoke defeats it.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn prepare_v2(
    process: &mut Process,
    store: &mut Persist,
    record: &Record,
    request: &agent_run_domain::owner_v2::RequestV2,
    deadline: tokio::time::Instant,
    channel: &std::path::Path,
    session: Option<&str>,
    idle: bool,
    closing: bool,
) -> PreparedV2 {
    use agent_run_adapters::native_owner_v2::reply;
    if request.validate().is_err() {
        return PreparedV2::Reply(reply(
            request,
            "known_unsent",
            None,
            None,
            Some("invalid_request"),
        ));
    }
    // Deterministic target and route checks run before any reservation. Exact
    // prior outcomes are still replayed; an absent row remains absent.
    if let Some(reason) = refusal_v2(
        process, store, record, request, deadline, channel, session, idle, closing,
    )
    .unwrap_or(Some("unavailable"))
    {
        let response = match lookup_effect(store, request) {
            Ok(view) if view.state != "absent" => effect_reply(request, &view),
            Ok(_) => reply(request, "known_unsent", None, None, Some(reason)),
            Err(agent_run_domain::Error::Conflict) => {
                reply(request, "known_unsent", None, None, Some("conflict"))
            }
            Err(_) => reply(request, "unknown", None, None, Some("native_uncertain")),
        };
        return PreparedV2::Reply(response);
    }
    let enabled = agent_run_config::provider_config::ProviderConfig::load(store.home())
        .ok()
        .and_then(|(config, _)| config.managed_attempt_binding)
        .is_some_and(|settings| settings.enabled);
    let reservation = match process
        .observing(store.reserve_owner_effect(request, enabled))
        .await
    {
        Ok(value) => value,
        Err(agent_run_domain::Error::Conflict) => {
            return PreparedV2::Reply(conflict_reply(store, request));
        }
        Err(agent_run_domain::Error::Capacity) => {
            return PreparedV2::Reply(reply(request, "known_unsent", None, None, Some("capacity")));
        }
        Err(agent_run_domain::Error::Unsupported(_)) => {
            return PreparedV2::Reply(reply(
                request,
                "known_unsent",
                None,
                None,
                Some("unavailable"),
            ));
        }
        Err(_) => {
            return PreparedV2::Reply(reply(
                request,
                "unknown",
                None,
                None,
                Some("native_uncertain"),
            ));
        }
    };
    let Some(token) = reservation.dispatch_token else {
        return PreparedV2::Reply(effect_reply(request, &reservation.view));
    };
    let refused = refusal_v2(
        process, store, record, request, deadline, channel, session, idle, closing,
    )
    .unwrap_or(Some("unavailable"));
    if let Some(reason) = refused {
        return PreparedV2::Reply(
            finish_v2(
                process,
                store,
                request,
                &token,
                "known_unsent",
                None,
                None,
                Some(reason),
            )
            .await,
        );
    }
    match process
        .observing(store.invoke_owner_effect(request, &token))
        .await
    {
        Ok(()) => {}
        Err(agent_run_domain::Error::Conflict) => {
            return PreparedV2::Reply(conflict_reply(store, request));
        }
        Err(_) => {
            return PreparedV2::Reply(reply(
                request,
                "unknown",
                None,
                None,
                Some("native_uncertain"),
            ));
        }
    }
    if let Some(reason) = refusal_v2(
        process, store, record, request, deadline, channel, session, idle, closing,
    )
    .unwrap_or(Some("unavailable"))
    {
        return PreparedV2::Reply(
            finish_v2(
                process,
                store,
                request,
                &token,
                "known_unsent",
                None,
                None,
                Some(reason),
            )
            .await,
        );
    }
    PreparedV2::Invoke(token)
}

/// An owner reply is constructed only after its matching durable transition.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn finish_v2(
    process: &mut Process,
    store: &mut Persist,
    request: &agent_run_domain::owner_v2::RequestV2,
    token: &str,
    state: &str,
    protocol: Option<&str>,
    native_id: Option<&str>,
    reason: Option<&str>,
) -> agent_run_domain::owner_v2::ReplyV2 {
    match process
        .observing(store.finish_owner_effect(request, token, state, protocol, native_id, reason))
        .await
    {
        Ok(view) => effect_reply(request, &view),
        Err(agent_run_domain::Error::Conflict) => conflict_reply(store, request),
        Err(_) => agent_run_adapters::native_owner_v2::reply(
            request,
            "unknown",
            None,
            None,
            Some("native_uncertain"),
        ),
    }
}

#[cfg(all(test, target_os = "linux"))]
mod managed_tests {
    use super::*;
    use agent_run_adapters::{LaunchPlan, io::RpcDisposition};
    use agent_run_domain::{
        attempt_binding::{
            Actor, AttemptRef, Binding, CurrentReceipt, Incarnation, LineageStep,
            RoleQualification, Route, SuccessorProof,
        },
        catalog::{HarnessId, ResolvedLaunchAuthority},
        domain::{AgentId, StartRequest},
        owner_v2::{OutboxIntent, RequestV2, outbox_digest},
    };
    use serde_json::json;
    use std::{collections::BTreeMap, time::Duration};

    /// The native pipe, writer fence, qualification and exact target all meet in
    /// one synthetic run. A replay of an accepted effect cannot write twice.
    #[tokio::test]
    async fn managed_owner_lane_fences_and_deduplicates_native_input() {
        let home = tempfile::tempdir().unwrap();
        let root = home.path().canonicalize().unwrap();
        let socket_base = std::env::var_os("AGENT_RUN_TEST_SOCKET_TMP")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let socket_dir = tempfile::Builder::new()
            .prefix("r3-anchor-")
            .tempdir_in(socket_base)
            .unwrap();
        let channel = socket_dir.path().join("bridge.sock");
        let _listener = std::os::unix::net::UnixListener::bind(&channel).unwrap();
        let marker = root.join("native-writes");
        let mut process = Process::spawn(&LaunchPlan {
            binary:"/bin/sh".into(),args:vec!["-c".into(),"IFS= read -r input; printf 'write\\n' >> \"$1\"; printf '%s\\n' '{\"id\":1,\"result\":{\"turn\":{\"id\":\"new-turn\"}}}'; read stop; printf 'second\\n' >> \"$1\"; read hold; printf 'third\\n' >> \"$1\"; read keep".into(),"fixture".into(),marker.to_string_lossy().into_owned()],
            cwd:root.clone(),environment:BTreeMap::new(),initial_input:None,
        }).unwrap();
        let leader = process.owner.leader.as_ref().unwrap().clone();
        let kernel = agent_run_platform::process::kernel_incarnation(&leader).unwrap();
        let incarnation = Incarnation {
            host_id: "host".into(),
            boot_id: kernel.boot_id,
            pid_namespace_inode: kernel.pid_namespace_inode,
            uid: kernel.uid,
            pid: kernel.pid,
            start_ticks: kernel.start_ticks,
        };
        let mut db = agent_run_store::fixture::initialize(&root).unwrap();
        let sid = agent_run_store::store_identity::read(&db.conn).unwrap();
        let route = |store: &str, id: &str| Route {
            component_store_id: store.into(),
            route_id: id.into(),
            worker_uid: incarnation.uid,
        };
        let run = route(&sid.to_string(), "run");
        let bridge = route("00000000-0000-4000-8000-000000000001", "bridge");
        let pool = route("pool-store", "pool");
        let config = format!(
            r#"schema_version=2
[managed_attempt_binding]
enabled=true
role_name='managed'
pool_server='pool'
pool_executable_sha256='{frontend}'
bridge_hook_sha256='{hook}'
host_id='host'
pool_resident_executable='/opt/pool-resident'
pool_resident_sha256='{resident}'
[managed_attempt_binding.transport]
run_route={{component_store_id='{sid}',route_id='run',worker_uid={uid}}}
pool_route={{component_store_id='pool-store',route_id='pool',worker_uid={uid}}}
bridge_route={{component_store_id='00000000-0000-4000-8000-000000000001',route_id='bridge',worker_uid={uid}}}
producer='pool'
pool_socket='/opt/pool.sock'
bridge_socket='{channel}'
bridge_executable='/opt/bridge-daemon'
bridge_executable_sha256='{bridge}'
bridge_reader_credential_file='/opt/reader'
bridge_reader_credential_sha256='{credential}'
"#,
            frontend = "a".repeat(64),
            hook = "c".repeat(64),
            resident = "b".repeat(64),
            bridge = "d".repeat(64),
            credential = "e".repeat(64),
            channel = channel.display(),
            uid = incarnation.uid
        );
        std::fs::write(root.join("config.toml"), &config).unwrap();
        let config =
            agent_run_config::provider_config::ProviderConfig::parse(&config, &root).unwrap();
        let provider_request:agent_run_domain::provider_start::ProviderStartRequest = serde_json::from_value(json!({"provider":"fixture","model":"model","profile":"managed","task":"task","workdir":root,"explicit_finish":true})).unwrap();
        let request:StartRequest = serde_json::from_value(json!({"runtime":"fixture","model":"model","profile":"managed","task":"task","workdir":root,"explicit_finish":true})).unwrap();
        let role_digest = "b".repeat(64);
        let authority = ResolvedLaunchAuthority {
            provider: "fixture".parse().unwrap(),
            harness: HarnessId::Codex,
            connection: agent_run_domain::ProviderConnection::Native,
            model: "model".into(),
            effort: None,
            profile: "managed".into(),
            workdir: root.clone(),
            role_payload: json!({"config_revision":role_digest}),
            assets_sha256: "a".repeat(64).parse().unwrap(),
            eligible_accounts: vec![],
        };
        let identity = crate::service::ProviderLaunchIdentity {
            provider_identity_version: 2,
            replay_request_sha256: agent_run_domain::canonical::sha256_hex(
                &serde_json::to_value(&provider_request).unwrap(),
                true,
            ),
            provider_request,
            provider_config_sha256: "e".repeat(64),
            provider_config_snapshot: config.snapshot().unwrap(),
            provider_config: config,
            authority,
            system_route: None,
            runtime_home: None,
            snapshot_sha256: None,
        };
        let id = AgentId::new();
        let attempt_id = format!("{}:1", id.as_str());
        let launch_id = uuid::Uuid::new_v4().to_string();
        db.conn.execute("INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,config_revision,root_agent_id,sequence,runtime_session_id,identity_json) VALUES(?,'fixture','model','managed','task','task',?,?,'running',1,'cfg',?,1,'native',?)",rusqlite::params![id.as_str(),root.to_string_lossy(),serde_json::to_string(&request).unwrap(),id.as_str(),serde_json::to_string(&identity).unwrap()]).unwrap();
        db.conn.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,phase,ownership_active,process_identity,process_birth_time) VALUES(?,?,1,'running','{}',1,'spawning',1,?,?)",rusqlite::params![attempt_id,id.as_str(),leader.token,leader.birth]).unwrap();
        db.conn
            .execute(
                "INSERT INTO worker_capabilities(attempt_id,token_sha256,created_at) VALUES(?,?,1)",
                rusqlite::params![attempt_id, "f".repeat(64)],
            )
            .unwrap();
        db.conn
            .execute(
                "INSERT INTO worker_lifecycle(attempt_id,phase,transition_at) VALUES(?,'idle',1)",
                [&attempt_id],
            )
            .unwrap();
        db.conn.execute("INSERT INTO attempt_launches(spawn_key,execution_id,sequence,attempt_id,launch_id,state,boot_id,pid_namespace_inode,uid,pid,start_ticks) VALUES(?,?,1,?,?,'bound',?,?,?,?,?)",rusqlite::params![attempt_id,id.as_str(),attempt_id,launch_id,incarnation.boot_id,incarnation.pid_namespace_inode,incarnation.uid,incarnation.pid,incarnation.start_ticks]).unwrap();
        db.attempt_qualify_role(
            &id,
            &RoleQualification {
                launch_id: launch_id.clone(),
                attempt_id: attempt_id.clone(),
                harness: "codex".into(),
                role_digest,
                config_digest: "c".repeat(64),
                inventory_digest: "d".repeat(64),
                incarnation: incarnation.clone(),
            },
        )
        .unwrap();
        let attempt = AttemptRef {
            run_store_id: sid.clone(),
            lineage_root_id: id.clone(),
            execution_id: id.clone(),
            sequence: 1,
            attempt_id: attempt_id.clone(),
            launch_id,
        };
        let actor = Actor {
            address: "host/codex/native".into(),
            role: "root".into(),
            native_id: "native".into(),
            epoch: 1,
        };
        let receipt = CurrentReceipt {
            receipt_id: uuid::Uuid::new_v4().to_string(),
            purpose: "egress".into(),
            lineage_root_id: id.clone(),
            bound_execution_id: id.clone(),
            attempt: attempt.clone(),
            incarnation: incarnation.clone(),
            actor: actor.clone(),
            successor_proof: SuccessorProof {
                lineage_root_id: id.clone(),
                bound_execution_id: id.clone(),
                current_execution_id: id.clone(),
                chain: vec![LineageStep {
                    execution_id: id.clone(),
                    parent_execution_id: None,
                    sequence: 1,
                }],
            },
        };
        let mut old_receipt = receipt.clone();
        old_receipt.receipt_id = uuid::Uuid::new_v4().to_string();
        old_receipt.actor.address = "host/codex/old".into();
        db.conn.execute("INSERT INTO attempt_current_receipts(request_id,pool_route_json,fingerprint,receipt_id,receipt_json) VALUES(?,?,?,?,?)",rusqlite::params!["egress-old",serde_json::to_string(&pool).unwrap(),"a".repeat(64),old_receipt.receipt_id,serde_json::to_string(&old_receipt).unwrap()]).unwrap();
        db.conn.execute("INSERT INTO attempt_current_receipts(request_id,pool_route_json,fingerprint,receipt_id,receipt_json) VALUES(?,?,?,?,?)",rusqlite::params!["egress",serde_json::to_string(&pool).unwrap(),"a".repeat(64),receipt.receipt_id,serde_json::to_string(&receipt).unwrap()]).unwrap();
        let record = db.get(&id).unwrap();
        crate::service::ProviderLaunchIdentity::read(&record).unwrap();
        let prep = crate::writer_test_harness::prepare_supervisor(&mut db, &id);
        drop(db);
        let writer = crate::writer_test_harness::Harness::start(&root);
        let mut persist = writer.supervisor(&root, prep, &id).await;
        let binding = Binding {
            pool_store_id: "pool-store".into(),
            pool_id: "pool".into(),
            member_id: "member".into(),
            binding_generation: 1,
            attempt,
            actor,
            run_route: run,
            bridge_route: bridge.clone(),
        };
        let outbox = OutboxIntent {
            effect_id: "effect".into(),
            producer_scope: "scope".into(),
            event_key: "event".into(),
            payload_sha256: agent_run_platform::fs::sha256(b"hello"),
            payload_length: 5,
            target_binding: binding,
            expected_incarnation: incarnation,
            bridge_route: bridge,
            mode: "message".into(),
            retention: "keep_all".into(),
        };
        let mut request = RequestV2 {
            version: 2,
            thread_id: "native".into(),
            call_id: "first".into(),
            effect_digest: outbox_digest(&outbox).unwrap(),
            outbox_intent: outbox,
            text: "hello".into(),
            wake_only: true,
        };
        let reply = agent_run_adapters::native_owner_v2::reply(
            &request,
            "known_unsent",
            None,
            None,
            Some("qualification_pending"),
        );
        crate::attempt_wire::assert_reply_keys(
            &serde_json::to_value(reply).unwrap(),
            &[
                "version",
                "call_id",
                "effect_id",
                "effect_digest",
                "state",
                "protocol",
                "native_id",
                "reason",
            ],
        );
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        for case in [
            "foreign-incarnation",
            "foreign-launch",
            "bad-digest",
            "expired",
            "foreign-route",
            "missing-receipt",
            "old-receipt",
            "child-actor",
            "wrong-channel",
            "wrong-session",
            "closing",
        ] {
            let mut forged = request.clone();
            forged.outbox_intent.effect_id = case.into();
            match case {
                "foreign-incarnation" => forged.outbox_intent.expected_incarnation.start_ticks += 1,
                "foreign-launch" => {
                    forged.outbox_intent.target_binding.attempt.launch_id =
                        uuid::Uuid::new_v4().to_string()
                }
                "foreign-route" => {
                    forged.outbox_intent.target_binding.run_route.route_id = "foreign".into()
                }
                "missing-receipt" => {
                    forged.outbox_intent.target_binding.actor.address = "host/foreign".into()
                }
                "old-receipt" => {
                    forged.outbox_intent.target_binding.actor.address = "host/codex/old".into()
                }
                "child-actor" => forged.outbox_intent.target_binding.actor.role = "child".into(),
                _ => {}
            }
            forged.effect_digest = outbox_digest(&forged.outbox_intent).unwrap();
            if case == "bad-digest" {
                forged.effect_digest = "0".repeat(64);
            }
            let expires = if case == "expired" {
                tokio::time::Instant::now() - Duration::from_millis(1)
            } else {
                deadline
            };
            match prepare_v2(
                &mut process,
                &mut persist,
                &record,
                &forged,
                expires,
                if case == "wrong-channel" {
                    root.as_path()
                } else {
                    channel.as_path()
                },
                Some(if case == "wrong-session" {
                    "other"
                } else {
                    "native"
                }),
                true,
                case == "closing",
            )
            .await
            {
                PreparedV2::Reply(reply) => {
                    assert_eq!(reply.state, "known_unsent", "{case}");
                    assert_eq!(
                        reply.reason.as_deref(),
                        Some(match case {
                            "foreign-incarnation" => "actor_mismatch",
                            "foreign-launch" => "attempt_not_ready",
                            "missing-receipt" | "old-receipt" => "attempt_not_ready",
                            "foreign-route" | "wrong-channel" | "wrong-session" => "actor_mismatch",
                            "closing" => "finished",
                            "bad-digest" | "child-actor" => "invalid_request",
                            _ => "unavailable",
                        }),
                        "{case}"
                    );
                }
                PreparedV2::Invoke(_) => panic!("{case} must never invoke"),
            }
        }
        assert!(!marker.exists());
        assert_eq!(
            persist
                .conn
                .query_row("SELECT count(*) FROM owner_effects", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
        let mut aborted = request.clone();
        aborted.outbox_intent.effect_id = "abort-race".into();
        aborted.effect_digest = outbox_digest(&aborted.outbox_intent).unwrap();
        let reserved = persist.reserve_owner_effect(&aborted, true).await.unwrap();
        let token = reserved.dispatch_token.unwrap();
        let route = agent_run_store::owner_v2::EffectRoute {
            run_route: aborted.outbox_intent.target_binding.run_route.clone(),
            bridge_route: aborted.outbox_intent.bridge_route.clone(),
        };
        let fence = writer
            .broker()
            .lane("effect-abort-test")
            .local(
                agent_run_store::ops::AbortOwnerEffect {
                    store: sid.clone(),
                    producer_scope: aborted.outbox_intent.producer_scope.clone(),
                    effect_id: aborted.outbox_intent.effect_id.clone(),
                    effect_digest: aborted.effect_digest.clone(),
                    route,
                },
                crate::broker_writer::BROKER_BUDGET,
            )
            .await
            .unwrap();
        assert_eq!(fence.state, "aborted");
        assert!(matches!(
            persist.invoke_owner_effect(&aborted, &token).await,
            Err(agent_run_domain::Error::Conflict)
        ));
        let conflict = conflict_reply(&persist, &aborted);
        assert_eq!(conflict.state, "known_unsent");
        assert_eq!(conflict.reason.as_deref(), Some("conflict"));
        let mut post_reserve = request.clone();
        post_reserve.outbox_intent.effect_id = "disabled-after-reserve".into();
        post_reserve.effect_digest = outbox_digest(&post_reserve.outbox_intent).unwrap();
        let pending_token = persist
            .reserve_owner_effect(&post_reserve, true)
            .await
            .unwrap()
            .dispatch_token
            .unwrap();
        let config_path = root.join("config.toml");
        let original_config = std::fs::read_to_string(&config_path).unwrap();
        std::fs::write(
            &config_path,
            original_config.replace("enabled=true", "enabled=false"),
        )
        .unwrap();
        assert_eq!(
            refusal_v2(
                &process,
                &persist,
                &record,
                &post_reserve,
                deadline,
                &channel,
                Some("native"),
                true,
                false
            )
            .unwrap(),
            Some("unavailable")
        );
        let closed = finish_v2(
            &mut process,
            &mut persist,
            &post_reserve,
            &pending_token,
            "known_unsent",
            None,
            None,
            Some("unavailable"),
        )
        .await;
        assert_eq!(closed.state, "known_unsent");
        let mut disabled = request.clone();
        disabled.outbox_intent.effect_id = "disabled".into();
        disabled.effect_digest = outbox_digest(&disabled.outbox_intent).unwrap();
        match prepare_v2(
            &mut process,
            &mut persist,
            &record,
            &disabled,
            deadline,
            &channel,
            Some("native"),
            true,
            false,
        )
        .await
        {
            PreparedV2::Reply(reply) => assert_eq!(reply.reason.as_deref(), Some("unavailable")),
            PreparedV2::Invoke(_) => panic!("disabled managed delivery cannot invoke"),
        }
        std::fs::write(config_path, original_config).unwrap();
        assert!(!marker.exists());
        assert_eq!(
            persist
                .conn
                .query_row("SELECT count(*) FROM owner_effects", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            2
        );
        writer
            .broker()
            .lane("cancel-fence-test")
            .remote(
                agent_run_store::ops::EnqueueCommand {
                    execution: id.clone(),
                    kind: "cancel".into(),
                    payload: json!({}),
                },
                crate::broker_writer::BROKER_BUDGET,
            )
            .await
            .unwrap();
        let mut cancelled = request.clone();
        cancelled.outbox_intent.effect_id = "cancelled".into();
        cancelled.effect_digest = outbox_digest(&cancelled.outbox_intent).unwrap();
        match prepare_v2(
            &mut process,
            &mut persist,
            &record,
            &cancelled,
            deadline,
            &channel,
            Some("native"),
            true,
            false,
        )
        .await
        {
            PreparedV2::Reply(reply) => assert_eq!(reply.reason.as_deref(), Some("cancelled")),
            PreparedV2::Invoke(_) => panic!("pending cancel cannot invoke"),
        }
        let (command, _, _) = persist.claim_command(&id).await.unwrap().unwrap();
        persist
            .complete_command(&id, command, &json!({"accepted":true}))
            .await
            .unwrap();
        assert_eq!(
            persist
                .conn
                .query_row("SELECT count(*) FROM owner_effects", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            2
        );
        match prepare_v2(
            &mut process,
            &mut persist,
            &record,
            &request,
            deadline,
            &channel,
            Some("native"),
            false,
            false,
        )
        .await
        {
            PreparedV2::Reply(reply) => assert_eq!(reply.reason.as_deref(), Some("not_idle")),
            PreparedV2::Invoke(_) => panic!("busy native lane must refuse"),
        }
        assert!(!marker.exists());
        request.call_id = "second".into();
        let token = match prepare_v2(
            &mut process,
            &mut persist,
            &record,
            &request,
            deadline,
            &channel,
            Some("native"),
            true,
            false,
        )
        .await
        {
            PreparedV2::Invoke(token) => token,
            PreparedV2::Reply(reply) => panic!(
                "qualified native lane refused: {} {:?}",
                reply.state, reply.reason
            ),
        };
        let result = process
            .rpc_exchange_until(
                "turn/start",
                json!({"threadId":"native","input":[{"type":"text","text":"hello"}]}),
                deadline,
            )
            .await
            .unwrap();
        assert!(matches!(result, RpcDisposition::Replied(_)));
        let mut concurrent = request.clone();
        concurrent.outbox_intent.effect_id = "second-while-pending".into();
        concurrent.effect_digest = outbox_digest(&concurrent.outbox_intent).unwrap();
        match prepare_v2(
            &mut process,
            &mut persist,
            &record,
            &concurrent,
            deadline,
            &channel,
            Some("native"),
            false,
            false,
        )
        .await
        {
            PreparedV2::Reply(reply) => assert_eq!(reply.reason.as_deref(), Some("not_idle")),
            PreparedV2::Invoke(_) => panic!("a second owner input cannot enter a busy lane"),
        }
        assert_eq!(
            persist
                .conn
                .query_row(
                    "SELECT count(*) FROM owner_effects WHERE effect_id='second-while-pending'",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            0
        );
        let accepted = finish_v2(
            &mut process,
            &mut persist,
            &request,
            &token,
            "accepted",
            Some("codex_turn"),
            Some("new-turn"),
            None,
        )
        .await;
        assert_eq!(accepted.state, "accepted");
        request.call_id = "third".into();
        match prepare_v2(
            &mut process,
            &mut persist,
            &record,
            &request,
            deadline,
            &channel,
            Some("native"),
            true,
            false,
        )
        .await
        {
            PreparedV2::Reply(reply) => {
                assert_eq!(reply.state, "accepted");
                assert_eq!(reply.native_id.as_deref(), Some("new-turn"));
            }
            PreparedV2::Invoke(_) => panic!("accepted effect cannot be invoked twice"),
        }
        assert_eq!(std::fs::read_to_string(&marker).unwrap(), "write\n");
        let mut failed_turn = request.clone();
        failed_turn.outbox_intent.effect_id = "claude-worker-turn-error".into();
        failed_turn.effect_digest = outbox_digest(&failed_turn.outbox_intent).unwrap();
        let token = match prepare_v2(
            &mut process,
            &mut persist,
            &record,
            &failed_turn,
            deadline,
            &channel,
            Some("native"),
            true,
            false,
        )
        .await
        {
            PreparedV2::Invoke(token) => token,
            PreparedV2::Reply(reply) => panic!("worker-turn fixture refused: {}", reply.state),
        };
        process.text("{\"type\":\"user\"}\n").await.unwrap();
        let (call, answer) = agent_run_adapters::native_owner_v2::Call::fixture(
            failed_turn.clone(),
            deadline,
            channel.clone(),
        );
        assert!(
            crate::stream::owner_v2_after_write(
                &mut process,
                &mut persist,
                &AgentId::new(),
                call,
                "input".into(),
                token
            )
            .await
            .is_err()
        );
        assert_eq!(answer.await.unwrap().state, "unknown");
        assert_eq!(
            lookup_effect(&persist, &failed_turn).unwrap().state,
            "unknown"
        );
        let mut expired = request.clone();
        expired.outbox_intent.effect_id = "claude-deadline".into();
        expired.effect_digest = outbox_digest(&expired.outbox_intent).unwrap();
        let token = match prepare_v2(
            &mut process,
            &mut persist,
            &record,
            &expired,
            deadline,
            &channel,
            Some("native"),
            true,
            false,
        )
        .await
        {
            PreparedV2::Invoke(token) => token,
            PreparedV2::Reply(reply) => panic!("deadline fixture refused: {}", reply.state),
        };
        process.text("{\"type\":\"user\"}\n").await.unwrap();
        let (call, answer) = agent_run_adapters::native_owner_v2::Call::fixture(
            expired.clone(),
            tokio::time::Instant::now() - Duration::from_millis(1),
            channel.clone(),
        );
        crate::stream::owner_v2_expire(&mut process, &mut persist, call, &token).await;
        assert_eq!(answer.await.unwrap().state, "unknown");
        match prepare_v2(
            &mut process,
            &mut persist,
            &record,
            &expired,
            deadline,
            &channel,
            Some("native"),
            true,
            false,
        )
        .await
        {
            PreparedV2::Reply(reply) => assert_eq!(reply.state, "unknown"),
            PreparedV2::Invoke(_) => panic!("deadline-unknown effect cannot resend"),
        }
        let marker_start = std::time::Instant::now();
        while std::fs::read_to_string(&marker).unwrap_or_default() != "write\nsecond\nthird\n" {
            assert!(marker_start.elapsed() < Duration::from_secs(5));
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            process
                .owner
                .cleanup(Duration::from_secs(2))
                .await
                .unwrap()
                .confirmed
        );
        process.reap().await;
    }
}
