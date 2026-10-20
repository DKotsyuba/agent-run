//! Operator-only exact native recipient observation; never worker authority.

use crate::{
    Error, Result, agent_identity,
    domain::{AgentId, Constraint, Status},
    process,
    service::{ProviderLaunchIdentity, Service},
    state::{Record, Store},
};
use agent_run_domain::integration::{
    Communication, NativeIntegration, UnknownReason, UnsupportedReason,
};
use bridge_sdk::{
    ClientError,
    bridge_protocol::{
        ErrorClass, Host, NativeAddress, RecipientResolution, ResolveRecipientParams,
    },
};
use rusqlite::OptionalExtension;
use serde_json::{Value, json};

/// Complete local ownership fence around one external read. Private by design.
#[derive(Debug, Clone, PartialEq)]
struct Snapshot {
    /// Exact current owned attempt, never the newest unrelated execution.
    attempt: String,
    /// Adapter/supervisor evidence, including its exact native session and store.
    native: NativeIntegration,
    /// Owned native process leader observed in the kernel.
    pid: i32,
    /// Full-precision birth; Bridge receives only its floored seconds.
    birth: f64,
    /// Kernel PID-reuse fence; both token and birth must match.
    token: String,
    /// Supervisor parent whose own token and birth were verified.
    supervisor_pid: i32,
    /// Supervisor kernel PID-reuse fence.
    supervisor_token: String,
    /// Full-precision supervisor birth.
    supervisor_birth: f64,
}

/// One private SQL row: attempt id, optional native-evidence JSON, optional
/// kernel token, optional full Unix birth seconds, and optional lifecycle phase.
/// Missing historical fields represent unavailable evidence, never inferred facts.
type AttemptObservation = (
    String,
    Option<String>,
    Option<String>,
    Option<f64>,
    Option<String>,
);

/// Constructs a safe failed observation without exposing private facts.
fn unknown(reason: UnknownReason) -> Communication {
    Communication::Unknown { reason }
}

/// Constructs a safe unsupported observation without granting another route.
fn unsupported(reason: UnsupportedReason) -> Communication {
    Communication::Unsupported { reason }
}

/// Verifies both kernel identity fields and actual parent/group relationships.
/// This broker-side check uses the recorded supervisor PID, never its own PID.
fn alive(snapshot: &Snapshot) -> bool {
    let Ok(native) = process::inspect(snapshot.pid) else {
        return false;
    };
    let Ok(supervisor) = process::inspect(snapshot.supervisor_pid) else {
        return false;
    };
    !native.zombie
        && !supervisor.zombie
        && native.pid == snapshot.pid
        && native.ppid == snapshot.supervisor_pid
        && native.group == snapshot.pid
        && native.birth == snapshot.birth
        && native.token == snapshot.token
        && supervisor.birth == snapshot.supervisor_birth
        && supervisor.token == snapshot.supervisor_token
}

/// Reads only the selected execution and its sole current owned attempt.
/// Missing historical evidence is unknown; malformed durable evidence is a typed
/// integrity error. Terminal, cancellation and accepted finish prevent lookup.
fn capture(
    store: &Store,
    id: &AgentId,
) -> Result<(Record, std::result::Result<Snapshot, Communication>)> {
    let row = store.get(id)?;
    let refusal = if row.status.terminal() || row.finished_at.is_some() {
        Some(unknown(UnknownReason::Terminal))
    } else if row.status == Status::Cancelling || store.cancel_pending(id)? {
        Some(unknown(UnknownReason::Cancelled))
    } else if !row.request.explicit_finish {
        Some(unsupported(UnsupportedReason::LegacyCompletion))
    } else if row.request.required_constraints.iter().any(|constraint| {
        matches!(
            constraint,
            Constraint::ResearchToolsOnly
                | Constraint::UnixIpcIsolation
                | Constraint::McpIpcIsolation
        )
    }) {
        Some(unsupported(UnsupportedReason::RestrictedRole))
    } else if row.status != Status::Running {
        Some(unknown(UnknownReason::NotStarted))
    } else {
        None
    };
    if let Some(refusal) = refusal {
        return Ok((row, Err(refusal)));
    }
    let attempt: Option<AttemptObservation> = store.conn.query_row(
        "SELECT t.id,t.session_facts_json,t.process_identity,t.process_birth_time,l.phase FROM attempts t LEFT JOIN worker_lifecycle l ON l.attempt_id=t.id WHERE t.agent_id=?1 AND t.ownership_active=1 AND t.state='running' AND t.phase='spawning' AND t.finished_at IS NULL AND (SELECT COUNT(*) FROM attempts WHERE agent_id=?1 AND ownership_active=1)=1",
        [id.as_str()], |record| Ok((record.get(0)?,record.get(1)?,record.get(2)?,record.get(3)?,record.get(4)?))
    ).optional()?;
    let Some((attempt, Some(native), Some(token), Some(birth), lifecycle)) = attempt else {
        return Ok((row, Err(unknown(UnknownReason::Unavailable))));
    };
    if lifecycle.as_deref() == Some("closing") {
        return Ok((row, Err(unknown(UnknownReason::Closing))));
    }
    if !matches!(lifecycle.as_deref(), Some("running" | "idle")) {
        return Ok((row, Err(unknown(UnknownReason::Unavailable))));
    }
    let native: NativeIntegration = serde_json::from_str(&native)
        .map_err(|_| Error::Integrity("invalid native integration evidence".into()))?;
    native
        .validate()
        .map_err(|_| Error::Integrity("invalid native integration evidence".into()))?;
    if native.session.is_none() {
        return Ok((row, Err(unknown(UnknownReason::NotStarted))));
    }
    let frozen = ProviderLaunchIdentity::read(&row)?;
    let role = agent_run_config::role_plan::ResolvedRolePlan::from_payload(
        &frozen.authority.role_payload,
    )?;
    if role.required_constraints.iter().any(|constraint| {
        matches!(
            constraint,
            Constraint::ResearchToolsOnly
                | Constraint::UnixIpcIsolation
                | Constraint::McpIpcIsolation
        )
    }) {
        return Ok((row, Err(unsupported(UnsupportedReason::RestrictedRole))));
    }
    if native.harness != frozen.authority.harness || native.session != row.runtime_session_id {
        return Ok((row, Err(unknown(UnknownReason::IdentityChanged))));
    }
    let Some(owned) = store.remembered_processes("attempt", &attempt)? else {
        return Ok((row, Err(unknown(UnknownReason::Unavailable))));
    };
    let Some(leader) = owned.leader.as_ref() else {
        return Ok((row, Err(unknown(UnknownReason::Unavailable))));
    };
    let (Some(supervisor_pid), Some(supervisor_token), Some(supervisor_birth)) = (
        row.supervisor_pid,
        row.supervisor_identity.clone(),
        row.supervisor_birth_time,
    ) else {
        return Ok((row, Err(unknown(UnknownReason::Unavailable))));
    };
    if leader.pid <= 1
        || leader.ppid != supervisor_pid
        || leader.group != leader.pid
        || leader.zombie
        || leader.token != token
        || leader.birth != birth
        || !birth.is_finite()
        || birth < 1.0
        || birth >= i64::MAX as f64
        || row.process_group_id != Some(leader.pid)
        || native.store.canonicalize().ok().as_ref() != Some(&native.store)
    {
        return Ok((row, Err(unknown(UnknownReason::IdentityChanged))));
    }
    let snapshot = Snapshot {
        attempt,
        native,
        pid: leader.pid,
        birth,
        token,
        supervisor_pid,
        supervisor_token,
        supervisor_birth,
    };
    if !alive(&snapshot) {
        return Ok((row, Err(unknown(UnknownReason::Unavailable))));
    }
    Ok((row, Ok(snapshot)))
}

impl Service {
    /// Observes an exact lineage sequence through a separately configured Bridge
    /// read credential. Omission pins latest once; no admission or store write occurs.
    /// Storage/identity integrity errors propagate; uncertain external reads return
    /// finite safe reasons. Blocking OS/socket work runs off the async executor.
    pub async fn agent_integration(
        &self,
        agent_id: AgentId,
        sequence: Option<u32>,
    ) -> Result<Value> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || service.integration_sync(&agent_id, sequence))
            .await
            .map_err(|_| Error::Runtime("native integration observation failed".into()))?
    }

    /// Executes one pinned read and rechecks the complete local fence after Bridge.
    /// A failed/replaced identity never returns an address from an earlier snapshot.
    fn integration_sync(&self, agent_id: &AgentId, sequence: Option<u32>) -> Result<Value> {
        let store = Store::open(&self.home)?;
        let selected = match sequence {
            Some(sequence) => agent_identity::resolve_sequence(&store, agent_id, sequence)?,
            None => agent_identity::resolve(&store, agent_id, None)?,
        };
        let (row, before) = capture(&store, &selected.id)?;
        let before = match before {
            Ok(value) => value,
            Err(refusal) => return self.integration_result(&store, &row, refusal),
        };
        let (config, _) = self.current_provider_config()?;
        let Some(bridge) = config.delivery.bridge else {
            return self.integration_result(
                &store,
                &row,
                unsupported(UnsupportedReason::ObserverUnconfigured),
            );
        };
        let Some(token_file) = bridge.observation_token_file.as_ref() else {
            return self.integration_result(
                &store,
                &row,
                unsupported(UnsupportedReason::ObserverUnconfigured),
            );
        };
        let communication = match crate::delivery::bridge::client_with_token(&bridge, token_file) {
            Ok((client, _)) => {
                // Credential/config I/O cannot make an earlier local snapshot
                // authoritative: check the same tuple immediately before lookup.
                let (current, checked) = capture(&store, &selected.id)?;
                match checked {
                    Ok(checked) if checked == before => {}
                    Ok(_) => {
                        return self.integration_result(
                            &store,
                            &current,
                            unknown(UnknownReason::IdentityChanged),
                        );
                    }
                    Err(refusal) => return self.integration_result(&store, &current, refusal),
                }
                let host = match before.native.harness {
                    agent_run_domain::HarnessId::Codex => Host::Codex,
                    agent_run_domain::HarnessId::ClaudeCode => Host::ClaudeCode,
                    agent_run_domain::HarnessId::OpenCode => Host::OpenCode,
                };
                let result = client.resolve_recipient(
                    format!("ar-observe-{}", uuid::Uuid::new_v4()),
                    ResolveRecipientParams {
                        host,
                        native_session_id: before
                            .native
                            .session
                            .clone()
                            .expect("capture requires a session"),
                        expected_store: before
                            .native
                            .store
                            .to_str()
                            .expect("validated UTF-8 store")
                            .to_owned(),
                        expected_harness_pid: before.pid as u32,
                        expected_harness_start_time_secs: before.birth.floor() as u64,
                    },
                );
                match result {
                    Ok(result) => match result.resolution {
                        RecipientResolution::Resolved { address }
                            if result.sampled_at_ms > 0
                                && result.sampled_at_ms <= i64::MAX as u64
                                && NativeAddress::parse(&address).is_ok_and(|parsed| {
                                    parsed.host == bridge.host_id
                                        && parsed.harness == host
                                        && Some(&parsed.session) == before.native.session.as_ref()
                                        && parsed.canonical() == address
                                }) =>
                        {
                            Communication::Observed {
                                address,
                                sampled_at_ms: result.sampled_at_ms,
                            }
                        }
                        _ => unknown(UnknownReason::Unavailable),
                    },
                    Err(ClientError::Service(error))
                        if matches!(error.class, ErrorClass::Auth | ErrorClass::Forbidden) =>
                    {
                        unsupported(UnsupportedReason::ObserverForbidden)
                    }
                    Err(_) => unknown(UnknownReason::Unavailable),
                }
            }
            Err(_) => unknown(UnknownReason::Unavailable),
        };
        let (row, after) = capture(&store, &selected.id)?;
        let communication = match after {
            Ok(after) if before == after => communication,
            Ok(_) => unknown(UnknownReason::IdentityChanged),
            Err(reason) => reason,
        };
        self.integration_result(&store, &row, communication)
    }

    /// Projects the existing stable AgentView with private cleanup PID redacted.
    /// No process/session/store/token fields are added by integration observation.
    fn integration_result(
        &self,
        store: &Store,
        row: &Record,
        mut communication: Communication,
    ) -> Result<Value> {
        let mut agent = agent_identity::view(self.view(store, row)?)?;
        if matches!(communication, Communication::Observed { .. }) {
            // The public view samples lifecycle/process facts again; never emit
            // an address alongside newer contradictory evidence it observed.
            if agent["phase"] == "closing" {
                communication = unknown(UnknownReason::Closing);
            } else if agent["process_state"] != "alive" {
                communication = unknown(UnknownReason::Unavailable);
            }
        }
        if let Some(cleanup) = agent.get_mut("cleanup").and_then(Value::as_object_mut) {
            cleanup.insert("process_group_id".into(), Value::Null);
        }
        Ok(json!({"schema_version":1,"agent":agent,"communication":communication}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_run_config::provider_config::ProviderConfig;
    use agent_run_domain::{AccountRecord, AccountStatus, HarnessId, ProviderStartRequest};
    use bridge_sdk::bridge_protocol::{
        ApiError, Operation, OperationResult, ResolveRecipientResult, Response, RetryHint,
        decode_request, encode_response,
    };
    use std::{
        fs,
        os::unix::{fs::PermissionsExt, process::CommandExt},
        path::PathBuf,
        process::{Child, Command, Stdio},
    };
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    /// Disposable real owned process and durable provider records; no model is launched.
    struct Fixture {
        /// Home lifetime; all credentials/configuration are synthetic.
        temp: tempfile::TempDir,
        /// Service observes only this fixture home.
        service: Service,
        /// Exact first execution identity.
        id: AgentId,
        /// Exact owned attempt.
        attempt: String,
        /// Private child process, killed/reaped by this fixture only.
        child: Child,
    }

    impl Fixture {
        /// Builds a valid frozen role/admission and records native evidence through
        /// supported store operations, with this test process as its supervisor.
        fn new() -> Self {
            let temp = tempfile::tempdir().unwrap();
            let home = temp.path().canonicalize().unwrap();
            agent_run_store::fixture::initialize(&home).unwrap();
            fs::create_dir_all(home.join("profiles")).unwrap();
            fs::write(home.join("profiles/code.md"), "+++\nrevision='1'\nwrite=true\nnetwork=false\nallow_external_read_roots=false\nskills=[]\nmcp=[]\nrequired_constraints=[]\n+++\nCode.\n").unwrap();
            fs::write(home.join("config.toml"), format!("schema_version=2\n[harnesses.codex]\nbinary='/usr/bin/true'\nhome='{h}/codex'\n[harnesses.claude-code]\nbinary='/usr/bin/true'\nhome='{h}/claude'\n[providers.p]\nharness='claude-code'\nconnection={{kind='custom',endpoint='https://example.invalid/api',protocol='messages'}}\nauth_family='anthropic'\nlimits_source='none'\n[[providers.p.models]]\nid='model'\n[[providers.p.bindings]]\nlabel='test'\naccount='test-account'\n", h=home.display())).unwrap();
            let mut store = agent_run_store::fixture::open(&home).unwrap();
            store
                .register_account(&AccountRecord {
                    account_id: "test-account".parse().unwrap(),
                    auth_family: "anthropic".parse().unwrap(),
                    secret_ref: "env:SYNTHETIC_ONLY".parse().unwrap(),
                    status: AccountStatus::Enabled,
                })
                .unwrap();
            let (config, revision) = ProviderConfig::load(&home).unwrap();
            let catalog = config
                .resolve_catalog(store.list_accounts().unwrap())
                .unwrap();
            let mut request: ProviderStartRequest = serde_json::from_value(json!({"provider":"p","model":"model","profile":"code","task":"fixture","workdir":home,"request_id":"fixture"})).unwrap();
            request.validate().unwrap();
            let prepared =
                crate::service::prepare_provider(&config, &revision, &catalog, request).unwrap();
            let candidates = crate::capacity::provider_ranking::provider_candidates(
                &store,
                &catalog,
                &prepared.request.provider,
                &prepared.request.model,
                None,
                &Default::default(),
            )
            .unwrap();
            let admitted = store
                .admit_provider(
                    &prepared.request,
                    &prepared.effective,
                    &catalog,
                    &prepared.authority,
                    &candidates,
                    &prepared.identity,
                    8,
                    None,
                    None,
                )
                .unwrap();
            let child = Command::new("/bin/sleep")
                .arg("60")
                .process_group(0)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap();
            // Establish the cleanup owner before any subsequent fallible setup.
            let fixture = Self {
                temp,
                service: Service::new(home.clone()),
                id: admitted.agent_id.clone(),
                attempt: admitted.attempt_id.clone(),
                child,
            };
            let owned = process::OwnedProcess::capture(fixture.child.id() as i32);
            let leader = owned.leader.as_ref().unwrap();
            let supervisor = process::inspect(std::process::id() as i32).unwrap();
            store
                .record_owner(
                    &admitted.agent_id,
                    supervisor.pid,
                    &supervisor.token,
                    Some(supervisor.birth),
                    crate::domain::now(),
                )
                .unwrap();
            store
                .provider_spawning(&admitted.agent_id, &admitted.attempt_id)
                .unwrap();
            let native = NativeIntegration {
                harness: HarnessId::ClaudeCode,
                store: home.clone(),
                session: None,
            };
            store
                .provider_process_evidence(
                    &admitted.agent_id,
                    &admitted.attempt_id,
                    leader,
                    Some(&native),
                )
                .unwrap();
            store
                .remember_processes("attempt", &admitted.attempt_id, &owned.snapshot().unwrap())
                .unwrap();
            store.bind_attempt(&admitted.attempt_id);
            store
                .runtime_session(&admitted.agent_id, "native-session")
                .unwrap();
            store.running(&admitted.agent_id, leader.pid).unwrap();
            store
                .issue_worker_capability(
                    &admitted.agent_id,
                    &admitted.attempt_id,
                    &"a".repeat(64),
                    crate::domain::now(),
                )
                .unwrap();
            store
                .begin_worker_lifecycle(
                    &admitted.agent_id,
                    &admitted.attempt_id,
                    crate::domain::now(),
                )
                .unwrap();
            fixture
        }

        /// Opens an isolated fixture connection; tests may deliberately corrupt facts.
        fn store(&self) -> Store<'static> {
            agent_run_store::fixture::open(self.temp.path()).unwrap()
        }

        /// Configures a separate protected observer and accepts exactly one SDK read.
        /// `reply` may synchronously change fixture facts before the response, proving
        /// the observer checks ownership again after lookup. No native Bridge is used.
        fn bridge(
            &self,
            reply: impl FnOnce(
                &ResolveRecipientParams,
                &PathBuf,
            ) -> std::result::Result<ResolveRecipientResult, ApiError>
            + Send
            + 'static,
        ) -> tokio::task::JoinHandle<()> {
            let home = self.temp.path().canonicalize().unwrap();
            let socket = home.join("b.sock");
            let listener = tokio::net::UnixListener::bind(&socket).unwrap();
            fs::set_permissions(&socket, fs::Permissions::from_mode(0o600)).unwrap();
            let token = home.join("observer");
            fs::write(&token, "synthetic-observer-only").unwrap();
            fs::set_permissions(&token, fs::Permissions::from_mode(0o600)).unwrap();
            let original = fs::read_to_string(home.join("config.toml")).unwrap();
            fs::write(home.join("config.toml"),format!("{original}\n[delivery.bridge]\nregistration_socket={socket:?}\ncore_socket={socket:?}\ntoken_file={delivery:?}\nobservation_token_file={token:?}\nhost_id='local'\nclaude_store='delivery-parent-namespace'\n",delivery=home.join("absent-delivery-token"))).unwrap();
            tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut stream = BufReader::new(stream);
                let mut line = String::new();
                stream.read_line(&mut line).await.unwrap();
                let request = decode_request(line.trim_end().as_bytes()).unwrap();
                let Operation::ResolveRecipient(params) = &request.operation else {
                    panic!("only exact resolution is permitted");
                };
                assert_eq!(params.native_session_id, "native-session");
                assert_eq!(params.expected_store, home.to_str().unwrap());
                assert!(params.expected_harness_pid > 1);
                let response = match reply(params, &home) {
                    Ok(result) => Response::ok(
                        &request.request_id,
                        OperationResult::ResolveRecipient(result),
                    ),
                    Err(error) => Response::error(&request.request_id, error),
                };
                let mut bytes = encode_response(&response).unwrap();
                bytes.push(b'\n');
                let _ = stream.get_mut().write_all(&bytes).await;
            })
        }
    }

    impl Drop for Fixture {
        /// Reaps exactly the child created by this test, including assertion failure.
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }

    /// One canonical synthetic resolution, unrelated to the delivery namespace.
    fn resolved() -> ResolveRecipientResult {
        ResolveRecipientResult {
            sampled_at_ms: 1,
            resolution: RecipientResolution::Resolved {
                address: "local/claude-code/worker-private/native-session".into(),
            },
        }
    }

    /// Accepts a synthetic authenticated finish through the normal store operation,
    /// preserving the real immutable payload/lifecycle constraints in race fixtures.
    fn finish(store: &mut Store, id: &AgentId, attempt: &str) {
        store
            .accept_worker_finish(
                id,
                attempt,
                &"a".repeat(64),
                &agent_run_domain::worker::FinishRequest {
                    summary: "fixture complete".into(),
                    status: agent_run_domain::worker::FinishStatus::Done,
                },
                crate::domain::now(),
                agent_run_domain::worker::FinishGuard::Clean,
            )
            .unwrap();
    }

    /// Bounds a fixture exchange so an unexpectedly skipped lookup fails instead
    /// of hanging the suite; only this synthetic listener task is aborted.
    async fn bridge_finished(mut task: tokio::task::JoinHandle<()>) {
        let result = tokio::time::timeout(std::time::Duration::from_secs(10), &mut task).await;
        if result.is_err() {
            task.abort();
        }
        result.expect("fixture Bridge lookup deadline").unwrap();
    }

    /// Live explicit-finish proof uses the worker tuple and redacts private evidence.
    #[tokio::test]
    async fn integration_observes_exact_owned_attempt_and_separate_observer() {
        let fixture = Fixture::new();
        let task = fixture.bridge(|_, _| Ok(resolved()));
        let value = fixture
            .service
            .agent_integration(fixture.id.clone(), Some(1))
            .await
            .unwrap();
        bridge_finished(task).await;
        assert_eq!(
            value["communication"],
            json!({"state":"observed","address":"local/claude-code/worker-private/native-session","sampled_at_ms":1})
        );
        assert_eq!(value["agent"]["agent_id"], fixture.id.as_str());
        assert_eq!(value["agent"]["sequence"], 1);
        assert_eq!(value["agent"]["completion_mode"], "explicit_finish");
        let _: agent_run_domain::AgentView =
            serde_json::from_value(value["agent"].clone()).unwrap();
        for private in [
            "expected_store",
            "expected_harness_pid",
            "process_identity",
            "process_birth_time",
            "supervisor_pid",
            "session_facts_json",
            "synthetic-observer-only",
            "delivery-parent-namespace",
        ] {
            assert!(!value.to_string().contains(private), "{private}: {value}");
        }
        assert!(matches!(
            fixture
                .service
                .agent_integration(fixture.id.clone(), Some(99))
                .await,
            Err(Error::NotFound(_))
        ));
    }

    /// Ownership/session/store/sequence mismatches never return a stale address;
    /// malformed persisted evidence remains a typed integrity failure.
    #[tokio::test]
    async fn integration_refuses_stale_private_evidence_without_bridge() {
        let fixture = Fixture::new();
        let store = fixture.store();
        let original: String = store
            .conn
            .query_row(
                "SELECT session_facts_json FROM attempts WHERE id=?",
                [&fixture.attempt],
                |row| row.get(0),
            )
            .unwrap();
        for statement in [
            "UPDATE attempts SET session_facts_json=json_set(session_facts_json,'$.session','wrong-session')",
            "UPDATE attempts SET session_facts_json=json_set(session_facts_json,'$.store','/not-present-native-store')",
            "UPDATE attempts SET process_birth_time=process_birth_time+0.000001",
            "UPDATE attempts SET process_identity='stale-token'",
            "UPDATE attempts SET ownership_active=0",
            "UPDATE agents SET process_group_id=process_group_id+1",
            "UPDATE agents SET supervisor_birth_time=supervisor_birth_time+0.000001",
        ] {
            store.conn.execute(statement, []).unwrap();
            let value = fixture
                .service
                .agent_integration(fixture.id.clone(), Some(1))
                .await
                .unwrap();
            assert_eq!(value["communication"]["state"], "unknown", "{value}");
            assert!(value["communication"].get("address").is_none());
            let leader = process::inspect(fixture.child.id() as i32).unwrap();
            store
                .conn
                .execute(
                    "UPDATE attempts SET session_facts_json=?,process_birth_time=?,process_identity=?,ownership_active=1 WHERE id=?",
                    rusqlite::params![original, leader.birth, leader.token, fixture.attempt],
                )
                .unwrap();
            let supervisor = process::inspect(std::process::id() as i32).unwrap();
            store
                .conn
                .execute(
                    "UPDATE agents SET process_group_id=?,supervisor_birth_time=? WHERE id=?",
                    rusqlite::params![leader.pid, supervisor.birth, fixture.id.as_str()],
                )
                .unwrap();
        }
        store
            .conn
            .execute("UPDATE attempts SET session_facts_json='{}'", [])
            .unwrap();
        assert!(matches!(
            fixture
                .service
                .agent_integration(fixture.id.clone(), Some(1))
                .await,
            Err(Error::Integrity(_))
        ));
    }

    /// A finish accepted while Bridge is resolving invalidates the result afterward.
    #[tokio::test]
    async fn integration_rechecks_closing_after_bridge_lookup() {
        let fixture = Fixture::new();
        let (id, attempt) = (fixture.id.clone(), fixture.attempt.clone());
        let task = fixture.bridge(move |_, home| {
            finish(
                &mut agent_run_store::fixture::open(home).unwrap(),
                &id,
                &attempt,
            );
            Ok(resolved())
        });
        let value = fixture
            .service
            .agent_integration(fixture.id.clone(), None)
            .await
            .unwrap();
        bridge_finished(task).await;
        assert_eq!(
            value["communication"],
            json!({"state":"unknown","reason":"closing"})
        );
    }

    /// An explicit read-grant denial is unsupported; a foreign address stays unknown.
    #[tokio::test]
    async fn integration_refuses_bridge_grant_and_response_mismatch() {
        for forbidden in [false, true] {
            let fixture = Fixture::new();
            let task = fixture.bridge(move |_, _| {
                if forbidden {
                    Err(ApiError {
                        class: ErrorClass::Forbidden,
                        reason: "recipient_observation_forbidden".into(),
                        retry_hint: RetryHint::Never,
                        effect: None,
                        reconciliation: None,
                        detail: "Read refused".into(),
                    })
                } else {
                    Ok(ResolveRecipientResult {
                        sampled_at_ms: 1,
                        resolution: RecipientResolution::Resolved {
                            address: "foreign/claude-code/worker-private/native-session".into(),
                        },
                    })
                }
            });
            let value = fixture
                .service
                .agent_integration(fixture.id.clone(), Some(1))
                .await
                .unwrap();
            bridge_finished(task).await;
            assert_eq!(
                value["communication"],
                if forbidden {
                    json!({"state":"unsupported","reason":"observer_forbidden"})
                } else {
                    json!({"state":"unknown","reason":"unavailable"})
                }
            );
        }
    }

    /// Null, zero, overflow and unknown fields are protocol validation failures,
    /// rejected before any store is opened or process read can occur.
    #[tokio::test]
    async fn integration_selector_is_strict_before_io() {
        let service = Service::new(PathBuf::from("/missing-integration-fixture"));
        for extra in [
            json!({"sequence":null}),
            json!({"sequence":0}),
            json!({"sequence":4294967296_u64}),
            json!({"run_id":"secret"}),
        ] {
            let mut input = json!({"agent_id":"ag-20261017-120000-0123456789"});
            input
                .as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            assert!(matches!(
                crate::dispatch::call(&service, "agent_integration", input).await,
                Err(Error::Validation(_))
            ));
        }
    }

    /// Restricted/legacy/cancelled/closing/terminal executions do not contact
    /// Bridge, and absent observer configuration never borrows delivery authority.
    #[tokio::test]
    async fn integration_lifecycle_and_role_refusals_preserve_standalone_runs() {
        for (sql, expected) in [
            (
                "SELECT 1",
                json!({"state":"unsupported","reason":"observer_unconfigured"}),
            ),
            (
                "UPDATE agents SET request_json=json_set(request_json,'$.explicit_finish',json('false'))",
                json!({"state":"unsupported","reason":"legacy_completion"}),
            ),
            (
                "UPDATE agents SET request_json=json_set(request_json,'$.required_constraints',json('[\"research_tools_only\"]'))",
                json!({"state":"unsupported","reason":"restricted_role"}),
            ),
            (
                "UPDATE agents SET request_json=json_set(request_json,'$.required_constraints',json('[\"unix_ipc_isolation\"]'))",
                json!({"state":"unsupported","reason":"restricted_role"}),
            ),
            (
                "UPDATE agents SET status='cancelling'",
                json!({"state":"unknown","reason":"cancelled"}),
            ),
            ("finish", json!({"state":"unknown","reason":"closing"})),
            (
                "UPDATE agents SET status='failed',finished_at=1",
                json!({"state":"unknown","reason":"terminal"}),
            ),
        ] {
            let fixture = Fixture::new();
            let mut store = fixture.store();
            if sql == "finish" {
                finish(&mut store, &fixture.id, &fixture.attempt);
            } else {
                store.conn.execute_batch(sql).unwrap();
            }
            let before = store.revision().unwrap();
            let value = fixture
                .service
                .agent_integration(fixture.id.clone(), Some(1))
                .await
                .unwrap();
            assert_eq!(value["communication"], expected, "{sql}");
            assert_eq!(
                store.revision().unwrap(),
                before,
                "observation leaves lifecycle unchanged"
            );
            assert!(
                process::inspect(fixture.child.id() as i32).is_ok(),
                "observation never terminates a worker"
            );
        }
    }

    /// PID/full-birth fences are rechecked after a valid Bridge reply, independently
    /// of Bridge's seconds-only process comparison.
    #[tokio::test]
    async fn integration_rechecks_full_birth_after_bridge_lookup() {
        let fixture = Fixture::new();
        let task = fixture.bridge(|_, home| {
            agent_run_store::fixture::open(home)
                .unwrap()
                .conn
                .execute(
                    "UPDATE attempts SET process_birth_time=process_birth_time+0.000001",
                    [],
                )
                .unwrap();
            Ok(resolved())
        });
        let value = fixture
            .service
            .agent_integration(fixture.id.clone(), Some(1))
            .await
            .unwrap();
        bridge_finished(task).await;
        assert_eq!(
            value["communication"],
            json!({"state":"unknown","reason":"identity_changed"})
        );
    }

    /// An explicit retained sequence never follows a newer lineage tip.
    #[tokio::test]
    async fn integration_pins_exact_retained_sequence_instead_of_latest() {
        let mut fixture = Fixture::new();
        fixture.child.kill().unwrap();
        fixture.child.wait().unwrap();
        let store = fixture.store();
        store
            .conn
            .execute("UPDATE agents SET status='failed',finished_at=1", [])
            .unwrap();
        store
            .conn
            .execute(
                "UPDATE attempts SET ownership_active=0,phase='cleanup_complete'",
                [],
            )
            .unwrap();
        store.conn.execute("INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,config_revision,root_agent_id,sequence) SELECT 'ag-20261017-120001-0123456789',runtime,model,profile,task,task_summary,workdir,request_json,'starting',created_at,config_revision,id,2 FROM agents WHERE id=?",[fixture.id.as_str()]).unwrap();
        let old = fixture
            .service
            .agent_integration(fixture.id.clone(), Some(1))
            .await
            .unwrap();
        let latest = fixture
            .service
            .agent_integration(fixture.id.clone(), None)
            .await
            .unwrap();
        assert_eq!(old["agent"]["sequence"], 1);
        assert_eq!(old["communication"]["reason"], "terminal");
        assert_eq!(latest["agent"]["sequence"], 2);
        assert_eq!(latest["communication"]["reason"], "not_started");
        assert_eq!(old["agent"]["agent_id"], latest["agent"]["agent_id"]);
    }
}
