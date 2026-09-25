//! External endpoint reuse through durable gates and the real supervisor, without model calls.

use super::*;
use agent_run_core::managed_services::Manager;

/// A probe checks a live fixture and its ready marker; the attempted launcher only records a lost race.
fn external_config(pid: u32, ready: &Path, spawned: &Path) -> String {
    format!(
        r#"
[services.shared]
command="/bin/sh"
args=["-c",'touch "$1" "$2"',"fixture","{spawned}","{ready}"]
cwd="/tmp"
reuse_existing=true
idle_timeout_seconds=1
monitor_interval_seconds=5
stop_grace_seconds=0
readiness={{command="/bin/sh",args=["-c",'test "$AGENT_RUN_SERVICE_OWNERSHIP" = external && test -z "$AGENT_RUN_SERVICE_PID" && kill -0 "$1" && test -f "$2"',"probe","{pid}","{ready}"]}}
"#,
        ready = ready.display(),
        spawned = spawned.display()
    )
}

/// Waits at most eight seconds for this disposable home's single startup gate to become ready.
async fn ready(manager: &mut Manager, home: &Path) {
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            manager.tick().await.unwrap();
            let state: String = Store::open(home)
                .unwrap()
                .conn
                .query_row("SELECT state FROM agent_service_gates", [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_ne!(state, "failed");
            if state == "ready" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .expect("bounded external service warmup");
}

/// Runs the admitted fake-engine task through the production supervisor with a ten-second ceiling.
async fn finish(home: &Path, id: &AgentId) {
    let mut child = supervisor(home, id);
    let exit = tokio::time::timeout(Duration::from_secs(10), child.wait())
        .await
        .unwrap()
        .unwrap();
    let row = Store::open(home).unwrap().get(id).unwrap();
    assert!(
        exit.success(),
        "{}: {:?} {:?}",
        id,
        row.failure_kind,
        row.failure_text
    );
    assert_eq!(
        Store::open(home).unwrap().get(id).unwrap().status,
        Status::Succeeded
    );
}

/// A foreign process survives initial reuse, a lost launch race, broker restart and idle retirement.
#[tokio::test]
async fn managed_services_external_reuse_and_race_never_claim_or_stop_foreign_process() {
    for race in [false, true] {
        let mut foreign = Command::new("/bin/sleep")
            .arg("30")
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let pid = foreign.id().unwrap();
        let files = tempfile::tempdir().unwrap();
        let available = files.path().join("ready");
        let spawned = files.path().join("spawned");
        if !race {
            fs::write(&available, "ready").unwrap();
        }
        let (_temp, home) = home_with(&external_config(pid, &available, &spawned), &[]);
        let _cleanup = ServiceProcesses(home.clone());
        let service = Service::new(home.clone());
        let admitted = service
            .admit_provider_trusted(request(&home), candidates(committed(&home)))
            .unwrap();
        let id: AgentId = serde_json::from_value(admitted["agent_id"].clone()).unwrap();
        let mut manager = Manager::new(&home, env!("CARGO_BIN_EXE_agent-run").into()).unwrap();
        ready(&mut manager, &home).await;
        assert_eq!(
            spawned.exists(),
            race,
            "a ready external endpoint must skip launch"
        );
        let store = Store::open(&home).unwrap();
        let (ownership, root, cleanup): (String, Option<String>, Option<String>) = store.conn.query_row(
            "SELECT ownership,process_identity_json,cleanup_json FROM managed_service_generations", [],
            |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?)),
        ).unwrap();
        assert_eq!(ownership, "external");
        assert_eq!(root.is_some(), race);
        if race {
            let proof: serde_json::Value = serde_json::from_str(cleanup.as_ref().unwrap()).unwrap();
            assert_eq!(
                proof["confirmed"], true,
                "owned race loser must be cleaned first"
            );
        }
        let claimed: i64 = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM process_ownership WHERE json_extract(leader_json,'$.pid')=?",
                [pid],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            claimed, 0,
            "external identity is never claimed by the broker"
        );
        drop(store);
        drop(manager);
        let mut manager = Manager::new(&home, env!("CARGO_BIN_EXE_agent-run").into()).unwrap();
        manager.tick().await.unwrap();
        let report = agent_run_core::doctor::run(&home).unwrap();
        assert!(report
            .findings
            .iter()
            .any(|f| f.component == "service:shared"
                && f.severity == "info"
                && f.detail.contains("external")));
        finish(&home, &id).await;
        manager.tick().await.unwrap();
        if !race {
            // Recovery removed an interrupted probe after an earlier retirement
            // retained its unknown result. Confirmation must be recomputed now.
            Store::open(&home).unwrap().conn.execute(
                "UPDATE managed_service_generations SET state='unknown',cleanup_json='{\"external\":true,\"confirmed\":false,\"probes_gone\":false}'",[]
            ).unwrap();
        }
        Store::open(&home)
            .unwrap()
            .conn
            .execute(
                "UPDATE managed_service_generations SET idle_since=?",
                [agent_run::domain::now() - 2.0],
            )
            .unwrap();
        manager.tick().await.unwrap();
        let state: String = Store::open(&home)
            .unwrap()
            .conn
            .query_row("SELECT state FROM managed_service_generations", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(state, "stopped");
        assert!(
            foreign.try_wait().unwrap().is_none(),
            "idle retirement killed a foreign process"
        );
        foreign.kill().await.unwrap();
        foreign.wait().await.unwrap();
    }
}

/// If external discovery finds nothing, the ordinary owned bootstrap and idle cleanup still apply.
#[tokio::test]
async fn managed_services_external_miss_starts_and_stops_owned_service() {
    let (_temp, home) = home_with(
        r#"
[services.shared]
command="/bin/sleep"
args=["20"]
cwd="/tmp"
reuse_existing=true
idle_timeout_seconds=1
monitor_interval_seconds=1
stop_grace_seconds=0
readiness={command="/bin/sh",args=["-c",'test "$AGENT_RUN_SERVICE_OWNERSHIP" = managed && kill -0 "$AGENT_RUN_SERVICE_PID"']}
"#,
        &[],
    );
    let _cleanup = ServiceProcesses(home.clone());
    let service = Service::new(home.clone());
    let admitted = service
        .admit_provider_trusted(request(&home), candidates(committed(&home)))
        .unwrap();
    let id: AgentId = serde_json::from_value(admitted["agent_id"].clone()).unwrap();
    // A broker can die during external preflight, before any service was spawned.
    // The replacement must authorize its own bootstrap with its new identity.
    let mut retired = Command::new("/bin/sleep")
        .arg("20")
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let previous = agent_run::process::inspect(retired.id().unwrap() as i32).unwrap();
    retired.kill().await.unwrap();
    retired.wait().await.unwrap();
    let config = agent_run_config::provider_config::ProviderConfig::load(&home)
        .unwrap()
        .0;
    let definition = &config.services["shared"];
    Store::open(&home).unwrap().conn.execute(
        "INSERT INTO managed_service_generations(id,service_id,revision,definition_json,state,ownership,broker_identity_json,created_at) VALUES ('interrupted-preflight','shared',?,?,'starting','external',?,?)",
        rusqlite::params![definition.revision().unwrap(),serde_json::to_string(definition).unwrap(),serde_json::to_string(&previous).unwrap(),agent_run::domain::now()],
    ).unwrap();
    let mut manager = Manager::new(&home, env!("CARGO_BIN_EXE_agent-run").into()).unwrap();
    ready(&mut manager, &home).await;
    let store = Store::open(&home).unwrap();
    let (ownership, root): (String, String) = store
        .conn
        .query_row(
            "SELECT ownership,process_identity_json FROM managed_service_generations",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(ownership, "managed");
    let root: agent_run::process::Identity = serde_json::from_str(&root).unwrap();
    drop(store);
    finish(&home, &id).await;
    manager.tick().await.unwrap();
    Store::open(&home)
        .unwrap()
        .conn
        .execute(
            "UPDATE managed_service_generations SET idle_since=?",
            [agent_run::domain::now() - 2.0],
        )
        .unwrap();
    manager.tick().await.unwrap();
    assert!(matches!(
        agent_run::process::observe(Some(root.pid), Some(&root.token), Some(root.birth)),
        agent_run::process::ProcessState::Dead | agent_run::process::ProcessState::Reused
    ));
}

/// A real resident broker restart rechecks the external endpoint and preserves its generation without owning its PID.
#[tokio::test]
async fn managed_services_external_survives_broker_pid_change() {
    use agent_run::transport::socket;
    let mut foreign = Command::new("/bin/sleep")
        .arg("40")
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let files = tempfile::tempdir().unwrap();
    let available = files.path().join("ready");
    let spawned = files.path().join("spawned");
    fs::write(&available, "ready").unwrap();
    let config = external_config(foreign.id().unwrap(), &available, &spawned)
        .replace("idle_timeout_seconds=1", "idle_timeout_seconds=60");
    let (_temp, home) = home_with(&config, &[]);
    let _cleanup = ServiceProcesses(home.clone());
    let mut original = None;
    for turn in 0..2 {
        let mut broker = resident_broker(&home);
        broker_ready(&home, &mut broker).await;
        let mut request = request(&home);
        request.request_id = Some(format!("restart-{turn}"));
        request.timeout_seconds = Some(10.0);
        let admitted = socket::client(&home, "start", serde_json::to_value(request).unwrap())
            .await
            .unwrap();
        let id: AgentId = serde_json::from_value(admitted["agent_id"].clone()).unwrap();
        tokio::time::timeout(Duration::from_secs(12), async {
            loop {
                let row = Store::open(&home).unwrap().get(&id).unwrap();
                if row.status.terminal() {
                    assert_eq!(row.status, Status::Succeeded, "{:?}", row.failure_text);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(30)).await;
            }
        })
        .await
        .unwrap();
        let generation:String=Store::open(&home).unwrap().conn.query_row(
            "SELECT id FROM managed_service_generations WHERE ownership='external' AND state='ready'",[],|row|row.get(0),
        ).unwrap();
        if let Some((id, pid)) = &original {
            assert_eq!(id, &generation);
            assert_ne!(Some(*pid), broker.id());
        } else {
            original = Some((generation, broker.id().unwrap()));
        }
        broker.kill().await.unwrap();
        broker.wait().await.unwrap();
        assert!(foreign.try_wait().unwrap().is_none());
    }
    assert!(!spawned.exists());
    foreign.kill().await.unwrap();
    foreign.wait().await.unwrap();
}

/// Loss of an externally managed endpoint blocks a new real supervisor without launching a replacement under active leases.
#[tokio::test]
async fn managed_services_external_health_loss_blocks_new_harness() {
    let mut foreign = Command::new("/bin/sleep")
        .arg("20")
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let files = tempfile::tempdir().unwrap();
    let available = files.path().join("ready");
    let spawned = files.path().join("spawned");
    fs::write(&available, "ready").unwrap();
    let (_temp, home) = home_with(
        &external_config(foreign.id().unwrap(), &available, &spawned),
        &[],
    );
    let _cleanup = ServiceProcesses(home.clone());
    let service = Service::new(home.clone());
    let first = service
        .admit_provider_trusted(request(&home), candidates(committed(&home)))
        .unwrap();
    let first_id: AgentId = serde_json::from_value(first["agent_id"].clone()).unwrap();
    let mut manager = Manager::new(&home, env!("CARGO_BIN_EXE_agent-run").into()).unwrap();
    ready(&mut manager, &home).await;
    Store::open(&home)
        .unwrap()
        .conn
        .execute("UPDATE managed_service_generations SET checked_at=0", [])
        .unwrap();
    let stale = agent_run_core::managed_services::wait_for_gate(&home, &first_id)
        .await
        .unwrap_err();
    assert!(stale.to_string().contains("health is stale"));
    manager.tick().await.unwrap();
    foreign.kill().await.unwrap();
    foreign.wait().await.unwrap();
    Store::open(&home)
        .unwrap()
        .conn
        .execute("UPDATE managed_service_generations SET checked_at=0", [])
        .unwrap();
    manager.tick().await.unwrap();
    let mut next = request(&home);
    next.request_id = Some("after-health-loss".into());
    let admitted = service
        .admit_provider_trusted(next, candidates(committed(&home)))
        .unwrap();
    let id: AgentId = serde_json::from_value(admitted["agent_id"].clone()).unwrap();
    manager.tick().await.unwrap();
    let mut child = supervisor(&home, &id);
    assert!(!tokio::time::timeout(Duration::from_secs(8), child.wait())
        .await
        .unwrap()
        .unwrap()
        .success());
    let row = Store::open(&home).unwrap().get(&id).unwrap();
    assert_eq!(row.status, Status::Failed);
    assert!(row.runtime_session_id.is_none());
    assert!(!spawned.exists());
}

/// Qualifies real CodeGraph reuse against an explicitly selected live endpoint without launching or stopping it.
#[tokio::test]
#[ignore = "requires AGENT_RUN_CODEGRAPH_BUNDLE and AGENT_RUN_CODEGRAPH_PROJECT with a running daemon"]
async fn managed_services_external_real_codegraph_qualification() {
    let bundle = std::path::PathBuf::from(std::env::var_os("AGENT_RUN_CODEGRAPH_BUNDLE").unwrap());
    let project =
        std::path::PathBuf::from(std::env::var_os("AGENT_RUN_CODEGRAPH_PROJECT").unwrap());
    let before = fs::read(project.join(".codegraph/daemon.pid")).unwrap();
    let info: serde_json::Value = serde_json::from_slice(&before).unwrap();
    let identity = agent_run::process::inspect(info["pid"].as_i64().unwrap() as i32).unwrap();
    let probe = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../scripts/services/codegraph-probe.cjs")
        .canonicalize()
        .unwrap();
    // The launcher cannot create a daemon: successful admission proves the external path was used.
    let config = format!(
        r#"
[services.codegraph]
command="/usr/bin/false"
cwd="{project}"
reuse_existing=true
idle_timeout_seconds=1
readiness={{command="{node}",args=["{probe}","{project}","1.6.0"],timeout_seconds=5}}
"#,
        project = project.display(),
        node = bundle.join("node").display(),
        probe = probe.display()
    );
    let (_temp, home) = home_with(&config, &[]);
    let _cleanup = ServiceProcesses(home.clone());
    let service = Service::new(home.clone());
    let admitted = service
        .admit_provider_trusted(request(&home), candidates(committed(&home)))
        .unwrap();
    let id: AgentId = serde_json::from_value(admitted["agent_id"].clone()).unwrap();
    let mut manager = Manager::new(&home, env!("CARGO_BIN_EXE_agent-run").into()).unwrap();
    ready(&mut manager, &home).await;
    finish(&home, &id).await;
    manager.tick().await.unwrap();
    Store::open(&home)
        .unwrap()
        .conn
        .execute(
            "UPDATE managed_service_generations SET idle_since=?",
            [agent_run::domain::now() - 2.0],
        )
        .unwrap();
    manager.tick().await.unwrap();
    let (ownership, state, root): (String, String, Option<String>) = Store::open(&home)
        .unwrap()
        .conn
        .query_row(
            "SELECT ownership,state,process_identity_json FROM managed_service_generations",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        (ownership.as_str(), state.as_str(), root),
        ("external", "stopped", None)
    );
    assert_eq!(
        fs::read(project.join(".codegraph/daemon.pid")).unwrap(),
        before
    );
    assert_eq!(
        agent_run::process::observe(
            Some(identity.pid),
            Some(&identity.token),
            Some(identity.birth)
        ),
        agent_run::process::ProcessState::Alive
    );
}
