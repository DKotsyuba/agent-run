#![cfg(target_os = "linux")]
//! Synthetic same-writer admission and namespace recovery boundaries.
use agent_run_domain::{
    attempt_binding::{
        Actor, AdmissionRequest, AttemptRef, Binding, ConsumeReceipt, ConsumedRef, EgressCurrent,
        Incarnation, IngressCurrent, Observation, Route,
    },
    domain::AgentId,
};
use agent_run_store::{
    Store,
    attempt_binding::{CurrentConsume, NamespaceRequest, PoolSeal, TerminalOutcome},
};
use rusqlite::params;

struct Fixture {
    _temp: tempfile::TempDir,
    store: Store<'static>,
    request: AdmissionRequest,
    consume: ConsumeReceipt,
    incarnation: Incarnation,
}

#[test]
fn ingress_receipt_is_exact_and_not_renewed_after_bind_changes() {
    let f = fixture();
    let r = &f.request;
    let current = IngressCurrent {
        expected_store_id: r.expected_store_id.clone(),
        request_id: "current-request".into(),
        bridge_route: r.binding.bridge_route.clone(),
        pool_route: r.pool_route.clone(),
        run_route: r.binding.run_route.clone(),
        producer: "pool".into(),
        bridge_context_ref: "context".into(),
        invocation_key: "invocation".into(),
    };
    f.store
        .attempt_begin_current(&current, &r.binding.attempt.launch_id)
        .unwrap();
    assert_eq!(
        f.store
            .attempt_capacity_headroom()
            .unwrap()
            .active_current_used,
        1
    );
    let result = f
        .store
        .attempt_current_ingress(&current, &f.consume.observation, &f.incarnation)
        .unwrap();
    assert_eq!(
        f.store
            .attempt_capacity_headroom()
            .unwrap()
            .active_current_used,
        1
    );
    assert_eq!(result.attempt, r.binding.attempt);
    let lookup = f
        .store
        .attempt_current_receipt(&r.expected_store_id, &r.pool_route, &result.receipt_id)
        .unwrap()
        .unwrap();
    assert_eq!(lookup, result);
    f.store
        .conn
        .execute(
            "UPDATE attempts SET ownership_active=0 WHERE id=?",
            [&r.binding.attempt.attempt_id],
        )
        .unwrap();
    assert_eq!(
        f.store
            .attempt_capacity_headroom()
            .unwrap()
            .active_current_used,
        0
    );
    assert_eq!(
        f.store
            .attempt_current_ingress(&current, &f.consume.observation, &f.incarnation)
            .unwrap(),
        result
    );
    let mut changed = current.clone();
    changed.request_id = "new-request".into();
    assert!(
        f.store
            .attempt_begin_current(&changed, &r.binding.attempt.launch_id)
            .is_err()
    );
}

#[test]
fn egress_after_resume_without_ingress() {
    let f = fixture();
    let original = &f.request.binding.attempt;
    f.store
        .conn
        .execute(
            "UPDATE agents SET status='succeeded' WHERE id=?",
            [original.execution_id.as_str()],
        )
        .unwrap();
    f.store
        .conn
        .execute(
            "UPDATE attempts SET ownership_active=0 WHERE id=?",
            [&original.attempt_id],
        )
        .unwrap();
    let child = AgentId::new();
    let attempt = format!("{}:1", child.as_str());
    let launch = uuid::Uuid::new_v4().to_string();
    f.store.conn.execute("INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,config_revision,root_agent_id,parent_agent_id,sequence,runtime_session_id,identity_json) SELECT ?,'codex','model','managed','task','task','/fixture','{}','running',2,'cfg',?,?,2,'native-2',identity_json FROM agents WHERE id=?", params![child.as_str(),original.lineage_root_id.as_str(),original.execution_id.as_str(),original.execution_id.as_str()]).unwrap();
    f.store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,phase,ownership_active,process_identity,process_birth_time) SELECT ?,?,1,'running','{}',2,'spawning',1,process_identity,process_birth_time FROM attempts WHERE id=?",params![attempt,child.as_str(),original.attempt_id]).unwrap();
    f.store
        .conn
        .execute(
            "INSERT INTO worker_capabilities(attempt_id,token_sha256,created_at) VALUES(?,?,2)",
            params![attempt, "e".repeat(64)],
        )
        .unwrap();
    f.store
        .conn
        .execute(
            "INSERT INTO worker_lifecycle(attempt_id,phase,transition_at) VALUES(?,'idle',2)",
            [&attempt],
        )
        .unwrap();
    f.store.conn.execute("INSERT INTO attempt_launches(spawn_key,execution_id,sequence,attempt_id,launch_id,state,boot_id,pid_namespace_inode,uid,pid,start_ticks) VALUES(?,?,2,?,?,'bound',?,?,?,?,?)",params![attempt,child.as_str(),attempt,launch,f.incarnation.boot_id,f.incarnation.pid_namespace_inode,f.incarnation.uid,f.incarnation.pid,f.incarnation.start_ticks]).unwrap();
    f.store
        .attempt_qualify_role(
            &child,
            &agent_run_domain::attempt_binding::RoleQualification {
                launch_id: launch.clone(),
                attempt_id: attempt.clone(),
                harness: "codex".into(),
                role_digest: "b".repeat(64),
                config_digest: "c".repeat(64),
                inventory_digest: "d".repeat(64),
                incarnation: f.incarnation.clone(),
            },
        )
        .unwrap();
    let request = EgressCurrent {
        expected_store_id: f.request.expected_store_id.clone(),
        request_id: "egress-after-resume".into(),
        lineage_root_id: original.lineage_root_id.clone(),
        bound_execution_id: original.execution_id.clone(),
        bridge_route: f.request.binding.bridge_route.clone(),
        pool_route: f.request.pool_route.clone(),
        run_route: f.request.binding.run_route.clone(),
        producer: "pool".into(),
        host_id: "host".into(),
    };
    let begin = f.store.attempt_begin_egress(&request, true).unwrap();
    let candidate = begin.candidate.unwrap();
    assert_eq!(candidate.attempt.execution_id, child);
    assert_eq!(candidate.attempt.launch_id, launch);
    let mut actor = f.request.binding.actor.clone();
    actor.native_id = "native-2".into();
    actor.address = "host/codex/native-2".into();
    actor.epoch = 2;
    let receipt = f
        .store
        .attempt_finish_egress(&request, &candidate, &actor, &f.incarnation, true)
        .unwrap();
    assert_eq!(receipt.purpose, "egress");
    assert_eq!(receipt.successor_proof.chain.len(), 2);
    assert_eq!(
        f.store
            .attempt_begin_egress(&request, false)
            .unwrap()
            .receipt
            .unwrap(),
        receipt
    );
    let ingress = IngressCurrent {
        expected_store_id: request.expected_store_id.clone(),
        request_id: "rootless-ingress".into(),
        bridge_route: request.bridge_route.clone(),
        pool_route: request.pool_route.clone(),
        run_route: request.run_route.clone(),
        producer: request.producer.clone(),
        bridge_context_ref: "original-context".into(),
        invocation_key: "invocation".into(),
    };
    let mut observation = f.consume.observation.clone();
    observation.launch_id = launch.clone();
    observation.actor = actor;
    f.store.attempt_begin_current(&ingress, &launch).unwrap();
    assert!(
        f.store
            .attempt_begin_current(&ingress, &uuid::Uuid::new_v4().to_string())
            .is_err()
    );
    let resolved = f
        .store
        .attempt_current_ingress(&ingress, &observation, &f.incarnation)
        .unwrap();
    assert_eq!(resolved.lineage_root_id, original.lineage_root_id);
    assert_eq!(resolved.bound_execution_id, original.lineage_root_id);
    assert_eq!(resolved.attempt.execution_id, child);
    assert_eq!(resolved.attempt.sequence, 2);
    assert_eq!(resolved.successor_proof.chain.len(), 2);
}

#[test]
fn early_hook_before_bind_reuses_original_observation() {
    let f = fixture_state(false, false);
    let r = &f.request;
    let a = &r.binding.attempt;
    let current = IngressCurrent {
        expected_store_id: r.expected_store_id.clone(),
        request_id: "early".into(),
        bridge_route: r.binding.bridge_route.clone(),
        pool_route: r.pool_route.clone(),
        run_route: r.binding.run_route.clone(),
        producer: "pool".into(),
        bridge_context_ref: "context".into(),
        invocation_key: "invocation".into(),
    };
    f.store
        .attempt_begin_current(&current, &a.launch_id)
        .unwrap();
    let err = f
        .store
        .attempt_current_ingress(&current, &f.consume.observation, &f.incarnation)
        .unwrap_err();
    assert!(err.to_string().contains("attempt not ready"));
    f.store.conn.execute("UPDATE attempt_launches SET state='bound',boot_id=?,pid_namespace_inode=?,uid=?,pid=?,start_ticks=? WHERE attempt_id=?",params![f.incarnation.boot_id,f.incarnation.pid_namespace_inode,f.incarnation.uid,f.incarnation.pid,f.incarnation.start_ticks,a.attempt_id]).unwrap();
    assert!(
        f.store
            .attempt_current_ingress(&current, &f.consume.observation, &f.incarnation)
            .is_err()
    );
    let qualification = agent_run_domain::attempt_binding::RoleQualification {
        launch_id: a.launch_id.clone(),
        attempt_id: a.attempt_id.clone(),
        harness: "codex".into(),
        role_digest: "b".repeat(64),
        config_digest: "c".repeat(64),
        inventory_digest: "d".repeat(64),
        incarnation: f.incarnation.clone(),
    };
    f.store
        .attempt_qualify_role(&a.execution_id, &qualification)
        .unwrap();
    assert_eq!(
        f.store
            .attempt_current_ingress(&current, &f.consume.observation, &f.incarnation)
            .unwrap()
            .attempt,
        *a
    );
}

#[test]
fn consensus_voter_finished_after_vote() {
    let f = fixture();
    let r = &f.request;
    let a = &r.binding.attempt;
    let classify = |inspected| {
        f.store
            .attempt_consensus_current(
                &r.expected_store_id,
                &a.lineage_root_id,
                &a.execution_id,
                a,
                &f.incarnation,
                inspected,
                "host",
            )
            .unwrap()
    };
    assert_eq!(classify(Some(&f.incarnation)).state, "live");
    assert_eq!(classify(None).state, "unavailable");
    f.store
        .conn
        .execute(
            "UPDATE agents SET status='succeeded' WHERE id=?",
            [a.execution_id.as_str()],
        )
        .unwrap();
    f.store
        .conn
        .execute(
            "UPDATE attempts SET ownership_active=0 WHERE id=?",
            [&a.attempt_id],
        )
        .unwrap();
    assert_eq!(classify(None).state, "terminal_no_successor");
    let child = AgentId::new();
    f.store.conn.execute("INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,config_revision,root_agent_id,parent_agent_id,sequence,runtime_session_id) VALUES(?,'codex','model','role','task','task','/fixture','{}','running',2,'cfg',?,?,2,'native')",params![child.as_str(),a.lineage_root_id.as_str(),a.execution_id.as_str()]).unwrap();
    let child_attempt = format!("{}:1", child.as_str());
    let launch = uuid::Uuid::new_v4().to_string();
    f.store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,phase,ownership_active,process_identity,process_birth_time) SELECT ?,?,1,'running','{}',2,'spawning',1,process_identity,process_birth_time FROM attempts WHERE id=?",params![child_attempt,child.as_str(),a.attempt_id]).unwrap();
    f.store.conn.execute("INSERT INTO attempt_launches(spawn_key,execution_id,sequence,attempt_id,launch_id,state,boot_id,pid_namespace_inode,uid,pid,start_ticks) VALUES(?,?,2,?,?,'bound',?,?,?,?,?)",params![child_attempt,child.as_str(),child_attempt,launch,f.incarnation.boot_id,f.incarnation.pid_namespace_inode,f.incarnation.uid,f.incarnation.pid,f.incarnation.start_ticks]).unwrap();
    let superseded = classify(None);
    assert_eq!(superseded.state, "superseded");
    assert_eq!(superseded.last_attempt.unwrap().execution_id, child);
}
fn fixture() -> Fixture {
    fixture_state(true, true)
}
fn fixture_state(bound: bool, qualified: bool) -> Fixture {
    let temp = tempfile::tempdir().unwrap();
    let store = agent_run_store::fixture::initialize(temp.path()).unwrap();
    let sid = agent_run_store::store_identity::read(&store.conn).unwrap();
    let id = AgentId::new();
    let attempt = format!("{}:1", id.as_str());
    let launch = uuid::Uuid::new_v4().to_string();
    let leader = agent_run_platform::process::inspect(std::process::id() as i32).unwrap();
    let kernel = agent_run_platform::process::kernel_incarnation(&leader).unwrap();
    let incarnation = Incarnation {
        host_id: "host".into(),
        boot_id: kernel.boot_id,
        pid_namespace_inode: kernel.pid_namespace_inode,
        uid: kernel.uid,
        pid: kernel.pid,
        start_ticks: kernel.start_ticks,
    };
    store.conn.execute("INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,config_revision,root_agent_id,sequence,runtime_session_id) VALUES(?,'codex','model','role','task','task','/fixture','{}','running',1,'cfg',?,1,'native')",params![id.as_str(),id.as_str()]).unwrap();
    store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,phase,ownership_active,process_identity,process_birth_time) VALUES(?,?,1,'running','{}',1,'spawning',1,?,?)",params![attempt,id.as_str(),leader.token,leader.birth]).unwrap();
    store
        .conn
        .execute(
            "INSERT INTO worker_capabilities(attempt_id,token_sha256,created_at) VALUES(?,?,1)",
            params![attempt, "f".repeat(64)],
        )
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO worker_lifecycle(attempt_id,phase,transition_at) VALUES(?,'running',1)",
            [&attempt],
        )
        .unwrap();
    if bound {
        store.conn.execute("INSERT INTO attempt_launches(spawn_key,execution_id,sequence,attempt_id,launch_id,state,boot_id,pid_namespace_inode,uid,pid,start_ticks) VALUES(?,?,1,?,?,'bound',?,?,?,?,?)",params![attempt,id.as_str(),attempt,launch,incarnation.boot_id,incarnation.pid_namespace_inode,incarnation.uid,incarnation.pid,incarnation.start_ticks]).unwrap();
    } else {
        store.conn.execute("INSERT INTO attempt_launches(spawn_key,execution_id,sequence,attempt_id,launch_id,state) VALUES(?,?,1,?,?,'prepared')",params![attempt,id.as_str(),attempt,launch]).unwrap();
        store
            .conn
            .execute(
                "UPDATE attempt_launches SET state='spawning' WHERE attempt_id=?",
                [&attempt],
            )
            .unwrap();
    }
    let frozen = serde_json::json!({"provider_identity_version":2,"provider_config":{"managed_attempt_binding":{"enabled":true,"role_name":"managed","host_id":"host"}},"authority":{"profile":"managed","harness":"codex","role_payload":{"config_revision":"b".repeat(64)}}});
    store
        .conn
        .execute(
            "UPDATE agents SET identity_json=? WHERE id=?",
            params![frozen.to_string(), id.as_str()],
        )
        .unwrap();
    let qualification = agent_run_domain::attempt_binding::RoleQualification {
        launch_id: launch.clone(),
        attempt_id: attempt.clone(),
        harness: "codex".into(),
        role_digest: "b".repeat(64),
        config_digest: "c".repeat(64),
        inventory_digest: "d".repeat(64),
        incarnation: incarnation.clone(),
    };
    if qualified {
        store.attempt_qualify_role(&id, &qualification).unwrap();
        store.attempt_qualify_role(&id, &qualification).unwrap();
    }
    let bridge = Route {
        component_store_id: "00000000-0000-4000-8000-000000000001".into(),
        route_id: "bridge".into(),
        worker_uid: kernel.uid,
    };
    let run = Route {
        component_store_id: sid.to_string(),
        route_id: "run".into(),
        worker_uid: kernel.uid,
    };
    let pool = Route {
        component_store_id: "pool-store".into(),
        route_id: "pool".into(),
        worker_uid: kernel.uid,
    };
    let actor = Actor {
        address: "host/codex/native".into(),
        role: "root".into(),
        native_id: "native".into(),
        epoch: 1,
    };
    let source = ConsumedRef {
        bridge_route: bridge.clone(),
        producer: "pool".into(),
        consume_request_id: "consume".into(),
        invocation_key: "invocation".into(),
        security_digest: "a".repeat(64),
        issuer_epoch: "old-issuer".into(),
    };
    let request = AdmissionRequest {
        version: 1,
        expected_store_id: sid.clone(),
        request_id: "intent".into(),
        intent_id: "intent".into(),
        security_digest: "a".repeat(64),
        binding: Binding {
            pool_store_id: "pool-store".into(),
            pool_id: "pool".into(),
            member_id: "member".into(),
            binding_generation: 1,
            attempt: AttemptRef {
                run_store_id: sid,
                lineage_root_id: id.clone(),
                execution_id: id,
                sequence: 1,
                attempt_id: attempt,
                launch_id: launch.clone(),
            },
            actor: actor.clone(),
            run_route: run,
            bridge_route: bridge,
        },
        consumed_ref: source,
        policy: "admission_snapshot".into(),
        pool_route: pool,
    };
    let consume = ConsumeReceipt {
        producer: "pool".into(),
        consume_request_id: "consume".into(),
        invocation_key: "invocation".into(),
        security_digest: "a".repeat(64),
        issuer_epoch: "old-issuer".into(),
        consumed_at_ms: 2,
        observation: Observation {
            incarnation: incarnation.clone(),
            launch_id: launch,
            actor,
            native_call_id: "call".into(),
            tool_name: "mcp__pool__pool_get".into(),
            observed_at_ms: 1,
            expires_at_ms: 3,
        },
    };
    Fixture {
        _temp: temp,
        store,
        request,
        consume,
        incarnation,
    }
}

#[test]
fn admit_abort_delayed() {
    let f = fixture();
    let r = &f.request;
    f.store.attempt_begin_admission(r).unwrap();
    let aborted = f
        .store
        .attempt_abort_absent(
            &r.expected_store_id,
            &r.intent_id,
            &r.security_digest,
            &r.pool_route,
            None,
        )
        .unwrap();
    assert_eq!(aborted.state, "aborted_before_admit");
    assert!(
        f.store
            .attempt_admit(
                r,
                &CurrentConsume {
                    receipt: &f.consume,
                    inspected: &f.incarnation
                }
            )
            .is_err()
    );
    assert_eq!(
        f.store
            .attempt_admission_lookup(
                &r.expected_store_id,
                &r.pool_route,
                &r.intent_id,
                &r.security_digest
            )
            .unwrap()
            .unwrap(),
        aborted
    );
    let mut changed = r.clone();
    changed.security_digest = "b".repeat(64);
    changed.consumed_ref.security_digest = changed.security_digest.clone();
    assert!(f.store.attempt_begin_admission(&changed).is_err());
}

#[test]
fn disabled_switch_refuses_pending_only_inside_writer_but_keeps_receipts() {
    let f = fixture();
    let r = &f.request;
    let a = &r.binding.attempt;
    let current = IngressCurrent {
        expected_store_id: r.expected_store_id.clone(),
        request_id: "disabled-current".into(),
        bridge_route: r.binding.bridge_route.clone(),
        pool_route: r.pool_route.clone(),
        run_route: r.binding.run_route.clone(),
        producer: "pool".into(),
        bridge_context_ref: "context".into(),
        invocation_key: "invocation".into(),
    };
    f.store
        .attempt_begin_current_gated(&current, &a.launch_id, true)
        .unwrap();
    assert!(matches!(
        f.store
            .attempt_begin_current_gated(&current, &a.launch_id, false),
        Err(agent_run_domain::Error::Unsupported(_))
    ));
    assert!(matches!(
        f.store.attempt_current_ingress_gated(
            &current,
            &f.consume.observation,
            &f.incarnation,
            false
        ),
        Err(agent_run_domain::Error::Unsupported(_))
    ));
    let receipt = f
        .store
        .attempt_current_ingress_gated(&current, &f.consume.observation, &f.incarnation, true)
        .unwrap();
    assert_eq!(
        f.store
            .attempt_current_ingress_gated(&current, &f.consume.observation, &f.incarnation, false)
            .unwrap(),
        receipt
    );
    f.store.attempt_begin_admission_gated(r, true).unwrap();
    assert!(matches!(
        f.store.attempt_begin_admission_gated(r, false),
        Err(agent_run_domain::Error::Unsupported(_))
    ));
    let evidence = CurrentConsume {
        receipt: &f.consume,
        inspected: &f.incarnation,
    };
    assert!(matches!(
        f.store.attempt_admit_gated(r, &evidence, false),
        Err(agent_run_domain::Error::Unsupported(_))
    ));
    let admitted = f.store.attempt_admit_gated(r, &evidence, true).unwrap();
    assert_eq!(
        f.store.attempt_admit_gated(r, &evidence, false).unwrap(),
        admitted
    );
}

#[test]
fn managed_qualification_is_opt_in_and_exact_to_bound_launch() {
    let f = fixture_state(true, false);
    let a = &f.request.binding.attempt;
    f.store.attempt_begin_admission(&f.request).unwrap();
    let error = f
        .store
        .attempt_admit(
            &f.request,
            &CurrentConsume {
                receipt: &f.consume,
                inspected: &f.incarnation,
            },
        )
        .unwrap_err();
    assert!(error.to_string().contains("attempt not ready"));
    let q = agent_run_domain::attempt_binding::RoleQualification {
        launch_id: a.launch_id.clone(),
        attempt_id: a.attempt_id.clone(),
        harness: "codex".into(),
        role_digest: "b".repeat(64),
        config_digest: "c".repeat(64),
        inventory_digest: "d".repeat(64),
        incarnation: f.incarnation.clone(),
    };
    f.store.conn.execute("UPDATE agents SET identity_json=json_set(identity_json,'$.provider_config.managed_attempt_binding.enabled',json('false')) WHERE id=?",[a.execution_id.as_str()]).unwrap();
    assert!(f.store.attempt_qualify_role(&a.execution_id, &q).is_err());
    f.store.conn.execute("UPDATE agents SET identity_json=json_set(identity_json,'$.provider_config.managed_attempt_binding.enabled',json('true')) WHERE id=?",[a.execution_id.as_str()]).unwrap();
    let mut wrong = q.clone();
    wrong.incarnation.start_ticks += 1;
    assert!(
        f.store
            .attempt_qualify_role(&a.execution_id, &wrong)
            .is_err()
    );
    f.store.attempt_qualify_role(&a.execution_id, &q).unwrap();
    let mut changed = q.clone();
    changed.inventory_digest = "e".repeat(64);
    assert!(
        f.store
            .attempt_qualify_role(&a.execution_id, &changed)
            .is_err()
    );
}

#[test]
fn admission_replay_settles_without_renewing_authority() {
    let f = fixture();
    let r = &f.request;
    f.store.attempt_begin_admission(r).unwrap();
    let first = f
        .store
        .attempt_admit(
            r,
            &CurrentConsume {
                receipt: &f.consume,
                inspected: &f.incarnation,
            },
        )
        .unwrap();
    // A durable replay does not read a later observation as fresh authority.
    let mut historical = f.consume.clone();
    historical.observation.actor.epoch = 2;
    assert_eq!(
        f.store
            .attempt_admit(
                r,
                &CurrentConsume {
                    receipt: &historical,
                    inspected: &f.incarnation
                }
            )
            .unwrap(),
        first
    );
    let id = &first.admission.as_ref().unwrap().admission_id;
    let digest = agent_run_domain::attempt_binding::settlement_digest(
        "pool-store",
        &r.intent_id,
        &r.security_digest,
        "committed",
    )
    .unwrap();
    let terminal = TerminalOutcome {
        pool_route: &r.pool_route,
        intent_id: &r.intent_id,
        security_digest: &r.security_digest,
        state: "committed",
        outcome_digest: &digest,
    };
    let settled = f
        .store
        .attempt_settle(&r.expected_store_id, id, &terminal)
        .unwrap();
    assert_eq!(settled.state, "settled_committed");
    assert_eq!(
        f.store
            .attempt_settle(&r.expected_store_id, id, &terminal)
            .unwrap(),
        settled
    );
    let bad = TerminalOutcome {
        state: "aborted",
        ..terminal
    };
    assert!(
        f.store
            .attempt_settle(&r.expected_store_id, id, &bad)
            .is_err()
    );
}

#[test]
fn foreign_actor_process_uid_and_reused_launch_refuse() {
    let f = fixture();
    let r = &f.request;
    f.store.attempt_begin_admission(r).unwrap();
    for kind in ["actor", "uid", "ticks", "launch", "issuer", "digest"] {
        let mut receipt = f.consume.clone();
        let mut inspected = f.incarnation.clone();
        match kind {
            "actor" => receipt.observation.actor.role = "child".into(),
            "uid" => {
                receipt.observation.incarnation.uid =
                    receipt.observation.incarnation.uid.wrapping_add(1);
                inspected.uid = receipt.observation.incarnation.uid;
            }
            "ticks" => {
                receipt.observation.incarnation.start_ticks += 1;
                inspected.start_ticks += 1;
            }
            "launch" => receipt.observation.launch_id = uuid::Uuid::new_v4().to_string(),
            "issuer" => receipt.issuer_epoch = "foreign-issuer".into(),
            "digest" => receipt.security_digest = "f".repeat(64),
            _ => unreachable!(),
        }
        let error = f
            .store
            .attempt_admit(
                r,
                &CurrentConsume {
                    receipt: &receipt,
                    inspected: &inspected,
                },
            )
            .unwrap_err();
        if matches!(kind, "uid" | "ticks" | "launch") {
            assert!(
                error.to_string().contains("attempt mismatch"),
                "{kind}: {error}"
            );
        }
        assert!(
            f.store
                .attempt_admission_lookup(
                    &r.expected_store_id,
                    &r.pool_route,
                    &r.intent_id,
                    &r.security_digest
                )
                .unwrap()
                .is_none()
        );
    }
    assert_eq!(
        f.store
            .attempt_admit(
                r,
                &CurrentConsume {
                    receipt: &f.consume,
                    inspected: &f.incarnation
                }
            )
            .unwrap()
            .state,
        "admitted"
    );
}

#[test]
fn current_exact_distinguishes_unready_actor_and_stale_binding() {
    let f = fixture();
    let mut request = f.request.clone();
    let mut receipt = f.consume.clone();
    request.binding.actor.native_id = "foreign-native".into();
    receipt.observation.actor = request.binding.actor.clone();
    f.store.attempt_begin_admission(&request).unwrap();
    let evidence = CurrentConsume {
        receipt: &receipt,
        inspected: &f.incarnation,
    };
    let error = f.store.attempt_admit(&request, &evidence).unwrap_err();
    assert!(error.to_string().contains("actor mismatch"), "{error}");

    let f = fixture();
    f.store.attempt_begin_admission(&f.request).unwrap();
    f.store
        .conn
        .execute(
            "DELETE FROM worker_lifecycle WHERE attempt_id=?",
            [&f.request.binding.attempt.attempt_id],
        )
        .unwrap();
    let evidence = CurrentConsume {
        receipt: &f.consume,
        inspected: &f.incarnation,
    };
    let error = f.store.attempt_admit(&f.request, &evidence).unwrap_err();
    assert!(error.to_string().contains("attempt not ready"), "{error}");

    let f = fixture();
    f.store.attempt_begin_admission(&f.request).unwrap();
    f.store
        .conn
        .execute(
            "UPDATE attempts SET ownership_active=0 WHERE id=?",
            [&f.request.binding.attempt.attempt_id],
        )
        .unwrap();
    let evidence = CurrentConsume {
        receipt: &f.consume,
        inspected: &f.incarnation,
    };
    let error = f.store.attempt_admit(&f.request, &evidence).unwrap_err();
    assert!(error.to_string().contains("binding stale"), "{error}");
}

#[test]
fn bridge_namespace_retirement_requires_recovery_and_exact_seal() {
    let f = fixture();
    let r = &f.request;
    f.store.attempt_begin_admission(r).unwrap();
    let namespace = NamespaceRequest {
        expected_store_id: r.expected_store_id.clone(),
        request_id: "retire-request".into(),
        pool_route: r.pool_route.clone(),
        bridge_route: r.consumed_ref.bridge_route.clone(),
        producer: "pool".into(),
        issuer_epoch: "old-issuer".into(),
        retirement_key: "retire-key".into(),
        pool_seal_request_id: "seal-request".into(),
    };
    f.store.attempt_retirement_prepare(&namespace).unwrap();
    assert!(
        f.store
            .attempt_retirement_fence(&namespace, "old-issuer")
            .is_err()
    );
    assert_eq!(
        f.store
            .attempt_retirement_fence(&namespace, "current-issuer")
            .unwrap()
            .state,
        "fenced"
    );
    f.store.attempt_begin_admission(r).unwrap(); // original-key recovery survives the fence
    let mut different = r.clone();
    different.intent_id = "another".into();
    different.request_id = "another".into();
    assert!(f.store.attempt_begin_admission(&different).is_err());
    let mut seal = PoolSeal {
        version: 1,
        store_id: "pool-store".into(),
        request_id: "seal-request".into(),
        seal_id: "seal-id".into(),
        bridge_route: namespace.bridge_route.clone(),
        producer: "pool".into(),
        issuer_epoch: "old-issuer".into(),
        retirement_key: "retire-key".into(),
        state: "draining".into(),
    };
    assert!(
        f.store
            .attempt_retirement_invoke(&namespace, &seal)
            .is_err()
    );
    f.store
        .attempt_retirement_observe_seal(&namespace, &seal)
        .unwrap();
    let mut changed_seal = seal.clone();
    changed_seal.seal_id = "different-seal".into();
    assert!(
        f.store
            .attempt_retirement_observe_seal(&namespace, &changed_seal)
            .is_err()
    );
    seal.state = "sealed".into();
    assert!(
        f.store
            .attempt_retirement_invoke(&namespace, &seal)
            .is_err()
    ); // pending work
    let admitted = f
        .store
        .attempt_admit(
            r,
            &CurrentConsume {
                receipt: &f.consume,
                inspected: &f.incarnation,
            },
        )
        .unwrap();
    assert!(
        f.store
            .attempt_retirement_invoke(&namespace, &seal)
            .is_err()
    ); // not settled
    let digest = agent_run_domain::attempt_binding::settlement_digest(
        "pool-store",
        &r.intent_id,
        &r.security_digest,
        "aborted",
    )
    .unwrap();
    let outcome = TerminalOutcome {
        pool_route: &r.pool_route,
        intent_id: &r.intent_id,
        security_digest: &r.security_digest,
        state: "aborted",
        outcome_digest: &digest,
    };
    f.store
        .attempt_settle(
            &r.expected_store_id,
            &admitted.admission.unwrap().admission_id,
            &outcome,
        )
        .unwrap();
    assert_eq!(
        f.store
            .attempt_retirement_invoke(&namespace, &seal)
            .unwrap()
            .state,
        "invoking"
    );
    assert_eq!(
        f.store
            .attempt_retirement_invoke(&namespace, &seal)
            .unwrap()
            .state,
        "invoking"
    );
    let retired = f
        .store
        .attempt_retirement_complete(&namespace, "seal-id", 2)
        .unwrap();
    assert_eq!(retired.state, "retired");
    assert_eq!(
        f.store
            .attempt_retirement_complete(&namespace, "seal-id", 2)
            .unwrap(),
        retired
    );
    assert!(
        f.store
            .attempt_retirement_complete(&namespace, "seal-id", 3)
            .is_err()
    );
}

#[test]
fn permanent_attempt_fences_cannot_be_deleted() {
    let f = fixture();
    let sql = [
        "INSERT INTO attempt_pending_checks(intent_id,security_digest,request_fingerprint,pool_route_json,bridge_route_json,producer,issuer_epoch,state) VALUES('i',printf('%064d',0),printf('%064d',0),'{}','{}','p','e','pending')",
        "INSERT INTO attempt_admissions(intent_id,security_digest,pool_route_json,state) VALUES('i',printf('%064d',0),'{}','aborted_before_admit')",
        "INSERT INTO attempt_pending_current(request_id,pool_route_json,bridge_route_json,producer,fingerprint,state) VALUES('r','{}','{}','p',printf('%064d',0),'pending')",
        "INSERT INTO attempt_current_receipts(request_id,pool_route_json,fingerprint,receipt_id,receipt_json) VALUES('r','{}',printf('%064d',0),'id','{}')",
        "INSERT INTO attempt_namespace_retirements(retirement_key,request_id,pool_seal_request_id,bridge_route_json,pool_route_json,producer,issuer_epoch,state) VALUES('k','r','s','{}','{}','p','e','prepared')",
        "INSERT INTO owner_effects(producer_scope,effect_id,effect_digest,run_route_json,bridge_route_json,state) VALUES('s','e',printf('%064d',0),'{}','{}','aborted')",
    ];
    for statement in sql {
        f.store.conn.execute(statement, []).unwrap();
    }
    let headroom = f.store.attempt_capacity_headroom().unwrap();
    assert_eq!(headroom.admission_keys_used, 1);
    assert_eq!(headroom.retirements_used, 1);
    assert_eq!(headroom.qualifications_used, 1);
    assert_eq!(headroom.owner_effects_used, 1);
    for table in [
        "attempt_launches",
        "attempt_role_qualifications",
        "attempt_pending_checks",
        "attempt_admissions",
        "attempt_pending_current",
        "attempt_current_receipts",
        "attempt_namespace_retirements",
        "owner_effects",
    ] {
        assert!(
            f.store
                .conn
                .execute(&format!("DELETE FROM {table}"), [])
                .is_err(),
            "{table}"
        );
    }
}
