//! Synthetic initial-task read tests; no provider process or live service is started.

use crate::common;

use agent_run_domain::{
    Error, ProviderStartRequest, Sha256Digest,
    batch::{ExecutionRef, PinId},
    catalog::{HarnessId, ProviderConnection, ResolvedLaunchAuthority},
    domain::AgentId,
    initial_task::{
        InitialTaskMode, InitialTaskReason, InitialTaskRequest, InitialTaskResponse,
        MAX_PAGE_BYTES, MAX_TASK_BYTES,
    },
};
use agent_run_store::{Store, initial_task};
use rusqlite::params;
use serde_json::{Value, json};

/// One fake provider-v2 root with a valid first attempt carrying `task` in all three sources.
fn root(home: &common::Home, store: &mut Store<'_>, task: &str) -> (AgentId, ExecutionRef) {
    let mut request = home.request();
    request.task = task.into();
    let (id, _) = store
        .admit(&request, &home.config, &json!({}), None)
        .unwrap();
    let provider: ProviderStartRequest = serde_json::from_value(json!({"provider":"mock", "model":"fixture", "profile":"review", "task":task, "workdir":home.path, "explicit_finish":false})).unwrap();
    let mut role = json!({"role_name":"review"});
    role["config_revision"] = json!(agent_run_domain::canonical::sha256_hex(&role, true));
    let authority = ResolvedLaunchAuthority {
        provider: "mock".parse().unwrap(),
        harness: HarnessId::ClaudeCode,
        connection: ProviderConnection::Native,
        model: "fixture".into(),
        effort: None,
        profile: "review".into(),
        workdir: home.path.clone(),
        role_payload: role,
        assets_sha256: "0".repeat(64).parse().unwrap(),
        eligible_accounts: vec!["acct".parse().unwrap()],
    };
    let value = serde_json::to_value(&provider).unwrap();
    let identity = json!({"provider_identity_version":2,"provider_request":value,
        "replay_request_sha256":agent_run_domain::canonical::sha256_hex(&value,true),
        "authority":authority});
    store
        .conn
        .execute(
            "UPDATE agents SET identity_json=?,status='succeeded',finished_at=1 WHERE id=?",
            params![identity.to_string(), id.as_str()],
        )
        .unwrap();
    let attempt = store.create_attempt(&id, "failed", &json!({})).unwrap();
    store.conn.execute("UPDATE attempts SET selected_account_id='acct',phase='cleanup_complete',ownership_active=0,cleanup_proof_json=? WHERE id=?",
        params![json!({"confirmed":true,"group_gone":true,"descendants_gone":true}).to_string(),attempt]).unwrap();
    let reference = ExecutionRef::from_first_provider_attempt(&id, &attempt).unwrap();
    (id, reference)
}

fn pin(store: &Store<'_>, root: &AgentId) -> PinId {
    let pin = PinId::new();
    store
        .conn
        .execute(
            "INSERT INTO history_pins(pin_id,root_agent_id) VALUES(?,?)",
            params![pin.as_str(), root.as_str()],
        )
        .unwrap();
    pin
}

/// Rewrites all three sources consistently, keeping the frozen replay proof valid.
fn set_task(store: &Store<'_>, id: &AgentId, task: &str) {
    store
        .conn
        .execute(
            "UPDATE agents SET task=?1,request_json=json_set(request_json,'$.task',?1) WHERE id=?2",
            params![task, id.as_str()],
        )
        .unwrap();
    let raw: String = store
        .conn
        .query_row(
            "SELECT identity_json FROM agents WHERE id=?",
            [id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    let mut identity: Value = serde_json::from_str(&raw).unwrap();
    identity["provider_request"]["task"] = json!(task);
    identity["replay_request_sha256"] = json!(agent_run_domain::canonical::sha256_hex(
        &identity["provider_request"],
        true
    ));
    store
        .conn
        .execute(
            "UPDATE agents SET identity_json=? WHERE id=?",
            params![identity.to_string(), id.as_str()],
        )
        .unwrap();
}

fn strict(
    id: &AgentId,
    reference: &ExecutionRef,
    pin: &PinId,
    store: &Store<'_>,
) -> InitialTaskRequest {
    InitialTaskRequest {
        version: 1,
        mode: InitialTaskMode::Strict,
        expected_store_id: Some(agent_run_store::store_identity::read(&store.conn).unwrap()),
        agent_id: id.clone(),
        sequence: Some(1),
        execution_ref: Some(reference.clone()),
        pin_id: Some(pin.clone()),
        offset: 0,
        max_bytes: MAX_PAGE_BYTES,
        expected_sha256: None,
        expected_total_bytes: None,
    }
}

fn current(id: &AgentId) -> InitialTaskRequest {
    InitialTaskRequest {
        version: 1,
        mode: InitialTaskMode::Current,
        expected_store_id: None,
        agent_id: id.clone(),
        sequence: None,
        execution_ref: None,
        pin_id: None,
        offset: 0,
        max_bytes: MAX_PAGE_BYTES,
        expected_sha256: None,
        expected_total_bytes: None,
    }
}

fn read(store: &Store<'_>, request: &InitialTaskRequest) -> InitialTaskResponse {
    let response = initial_task::read(&store.conn, request).unwrap();
    response.validate_for(request).unwrap();
    response
}

/// Asserts a typed refusal and that no text accompanies it on the wire.
fn refused(store: &Store<'_>, request: &InitialTaskRequest, reason: InitialTaskReason) {
    match read(store, request) {
        InitialTaskResponse::Unavailable(value) => {
            assert_eq!(value.reason, reason);
            let wire = serde_json::to_value(InitialTaskResponse::Unavailable(value)).unwrap();
            assert_eq!(wire["state"], "unavailable");
            assert!(wire.get("text").is_none());
        }
        InitialTaskResponse::Available(_) => panic!("expected unavailable {reason:?}"),
    }
}

fn page(response: InitialTaskResponse) -> agent_run_domain::initial_task::InitialTaskPage {
    match response {
        InitialTaskResponse::Available(page) => page,
        InitialTaskResponse::Unavailable(value) => panic!("unavailable: {:?}", value.reason),
    }
}

/// Follows every `next_offset`, verifying fences on each page, and returns the buffered text.
fn buffer(store: &Store<'_>, mut request: InitialTaskRequest) -> (String, Vec<usize>) {
    let mut text = String::new();
    let mut sizes = Vec::new();
    loop {
        let result = page(read(store, &request));
        assert_eq!(result.offset, text.len());
        assert_eq!(result.text.len(), result.end - result.offset);
        assert!(result.text.len() <= request.max_bytes);
        sizes.push(result.text.len());
        text.push_str(&result.text);
        request.expected_sha256 = Some(result.sha256.clone());
        request.expected_total_bytes = Some(result.total_bytes);
        match result.next_offset {
            Some(next) => {
                assert!(!result.complete);
                request.offset = next;
            }
            None => {
                assert!(result.complete);
                assert_eq!(text.len(), result.total_bytes);
                let digest: Sha256Digest = agent_run_platform::fs::sha256(text.as_bytes())
                    .parse()
                    .unwrap();
                assert_eq!(digest, result.sha256);
                return (text, sizes);
            }
        }
    }
}

/// Both modes return the exact text with whole-text length and SHA on the single page.
#[test]
fn strict_and_current_return_exact_text_with_whole_text_fence() {
    let home = common::Home::new();
    let mut store = home.store();
    let task = "Исправь тест 🦀\nsecond line, \"quoted\" \\ backslash\ttab";
    let (id, reference) = root(&home, &mut store, task);
    let pin = pin(&store, &id);
    let expected: Sha256Digest = agent_run_platform::fs::sha256(task.as_bytes())
        .parse()
        .unwrap();
    let store_id = agent_run_store::store_identity::read(&store.conn).unwrap();

    let strict_page = page(read(&store, &strict(&id, &reference, &pin, &store)));
    assert_eq!(strict_page.mode, InitialTaskMode::Strict);
    assert_eq!(strict_page.store_id, store_id);
    assert_eq!(strict_page.agent_id, id);
    assert_eq!(strict_page.sequence, 1);
    assert_eq!(strict_page.execution_ref, reference);
    assert_eq!(strict_page.pin_id.as_ref(), Some(&pin));
    assert_eq!(strict_page.text, task);
    assert_eq!(strict_page.total_bytes, task.len());
    assert_eq!(strict_page.sha256, expected);
    assert_eq!((strict_page.offset, strict_page.end), (0, task.len()));
    assert!(strict_page.complete && strict_page.next_offset.is_none());

    // Current mode needs no pin and makes no pin claim, but still computes the ref.
    let current_page = page(read(&store, &current(&id)));
    assert_eq!(current_page.mode, InitialTaskMode::Current);
    assert_eq!(current_page.pin_id, None);
    assert_eq!(current_page.execution_ref, reference);
    assert_eq!(current_page.text, task);
    assert_eq!(current_page.sha256, expected);
    let wire = serde_json::to_value(InitialTaskResponse::Available(current_page)).unwrap();
    assert_eq!(wire["state"], "available");
    assert!(wire.get("pin_id").is_none());
    assert_eq!(wire["next_offset"], Value::Null);

    // A current-mode client may fence the identity it learned from its first page.
    let mut fenced = current(&id);
    fenced.execution_ref = Some(reference);
    assert_eq!(page(read(&store, &fenced)).text, task);
}

/// A foreign store identity is a typed error, never content or an unavailable state.
#[test]
fn wrong_store_is_a_typed_error() {
    let home = common::Home::new();
    let mut store = home.store();
    let (id, reference) = root(&home, &mut store, "task");
    let pin = pin(&store, &id);
    let other = common::Home::new();
    let foreign = agent_run_store::store_identity::read(&other.store().conn).unwrap();
    for mut request in [strict(&id, &reference, &pin, &store), current(&id)] {
        request.expected_store_id = Some(foreign.clone());
        assert!(matches!(
            initial_task::read(&store.conn, &request),
            Err(Error::StoreIdentityMismatch)
        ));
    }
    assert!(
        initial_task::read_in_snapshot(&store.conn, &current(&id)).is_err(),
        "caller must own a read snapshot"
    );
}

/// Pages split only at UTF-8 boundaries, including 2-, 3- and 4-byte scalars.
#[test]
fn paging_crosses_multibyte_boundaries_and_reassembles_exactly() {
    let home = common::Home::new();
    let mut store = home.store();
    let task = "a🦀é€z🦀🦀ж";
    let (id, reference) = root(&home, &mut store, task);
    let pin = pin(&store, &id);
    for max_bytes in 4..=9 {
        for mut request in [strict(&id, &reference, &pin, &store), current(&id)] {
            request.max_bytes = max_bytes;
            let (text, sizes) = buffer(&store, request);
            assert_eq!(text, task);
            assert!(sizes.iter().all(|size| *size >= 1 && *size <= max_bytes));
        }
    }
    // With a four-byte budget the crab at byte 1 does not fit after 'a': the page
    // rounds down to the boundary instead of splitting the scalar.
    let mut request = current(&id);
    request.max_bytes = 4;
    let first = page(read(&store, &request));
    assert_eq!(
        (first.text.as_str(), first.end, first.next_offset),
        ("a", 1, Some(1))
    );

    // An offset inside a scalar, at EOF or beyond it never yields a fragment.
    let total = task.len();
    let fence = |offset: usize| {
        let mut request = current(&id);
        request.offset = offset;
        request.expected_sha256 = Some(first.sha256.clone());
        request.expected_total_bytes = Some(total);
        request
    };
    assert!(!task.is_char_boundary(2) && !task.is_char_boundary(total - 1));
    assert!(matches!(
        initial_task::read(&store.conn, &fence(2)),
        Err(Error::Validation(_))
    ));
    assert!(matches!(
        initial_task::read(&store.conn, &fence(total - 1)),
        Err(Error::Validation(_))
    ));
    assert!(fence(total).validate().is_err());
}

/// The default page budget splits large multi-byte text, and 512 KiB exactly is allowed.
#[test]
fn size_limit_is_exact_and_never_truncates() {
    let home = common::Home::new();
    let mut store = home.store();
    let (id, reference) = root(&home, &mut store, "seed");
    let pin = pin(&store, &id);

    // 3-byte scalars straddle every 64 KiB boundary: 65536 is not a multiple of 3.
    let mut task = "€".repeat(MAX_TASK_BYTES / 3);
    task.push_str(&"x".repeat(MAX_TASK_BYTES - task.len()));
    assert_eq!(task.len(), MAX_TASK_BYTES);
    set_task(&store, &id, &task);
    let (text, sizes) = buffer(&store, strict(&id, &reference, &pin, &store));
    assert!(text == task);
    assert!(sizes.len() >= 8);
    assert!(sizes.iter().all(|size| *size <= MAX_PAGE_BYTES));

    // One byte more is refused whichever source carries it; nothing is cut.
    let mut over = task.clone();
    over.push('y');
    set_task(&store, &id, &over);
    refused(
        &store,
        &strict(&id, &reference, &pin, &store),
        InitialTaskReason::TaskTooLarge,
    );
    refused(&store, &current(&id), InitialTaskReason::TaskTooLarge);
    set_task(&store, &id, &task);
    store
        .conn
        .execute(
            "UPDATE agents SET request_json=json_set(request_json,'$.task',?) WHERE id=?",
            params![over, id.as_str()],
        )
        .unwrap();
    refused(&store, &current(&id), InitialTaskReason::TaskTooLarge);
}

/// Any disagreement among the three sources withholds the text.
#[test]
fn mismatch_between_task_sources_is_unavailable() {
    let home = common::Home::new();
    let mut store = home.store();
    let (id, reference) = root(&home, &mut store, "the real task");
    let pin = pin(&store, &id);
    let all = |store: &Store<'_>, reason| {
        refused(store, &strict(&id, &reference, &pin, store), reason);
        refused(store, &current(&id), reason);
    };

    store
        .conn
        .execute(
            "UPDATE agents SET task='the real task ' WHERE id=?",
            [id.as_str()],
        )
        .unwrap();
    all(&store, InitialTaskReason::TaskMismatch);
    set_task(&store, &id, "the real task");
    page(read(&store, &current(&id)));

    store
        .conn
        .execute(
            "UPDATE agents SET request_json=json_set(request_json,'$.task','other') WHERE id=?",
            [id.as_str()],
        )
        .unwrap();
    all(&store, InitialTaskReason::TaskMismatch);
    set_task(&store, &id, "the real task");

    // Frozen provider task alone differs while its replay proof stays internally valid.
    let raw: String = store
        .conn
        .query_row(
            "SELECT identity_json FROM agents WHERE id=?",
            [id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    let mut identity: Value = serde_json::from_str(&raw).unwrap();
    identity["provider_request"]["task"] = json!("tampered");
    identity["replay_request_sha256"] = json!(agent_run_domain::canonical::sha256_hex(
        &identity["provider_request"],
        true
    ));
    store
        .conn
        .execute(
            "UPDATE agents SET identity_json=? WHERE id=?",
            params![identity.to_string(), id.as_str()],
        )
        .unwrap();
    all(&store, InitialTaskReason::TaskMismatch);

    // A changed frozen request without a matching replay proof is invalid evidence.
    identity["replay_request_sha256"] = json!("0".repeat(64));
    store
        .conn
        .execute(
            "UPDATE agents SET identity_json=? WHERE id=?",
            params![identity.to_string(), id.as_str()],
        )
        .unwrap();
    all(&store, InitialTaskReason::ProviderIdentityInvalid);
    store
        .conn
        .execute(
            "UPDATE agents SET identity_json='{}' WHERE id=?",
            [id.as_str()],
        )
        .unwrap();
    all(&store, InitialTaskReason::ProviderIdentityInvalid);
}

/// Absent, non-text, blank or unparsable sources are missing, never guessed.
#[test]
fn missing_task_sources_are_unavailable() {
    let home = common::Home::new();
    let mut store = home.store();
    let (id, reference) = root(&home, &mut store, "task body");
    let pin = pin(&store, &id);
    for update in [
        "UPDATE agents SET request_json='{}'",
        "UPDATE agents SET request_json='{\"task\":7}'",
        "UPDATE agents SET task='  '",
    ] {
        set_task(&store, &id, "task body");
        store
            .conn
            .execute(&format!("{update} WHERE id=?"), [id.as_str()])
            .unwrap();
        refused(&store, &current(&id), InitialTaskReason::TaskMissing);
        refused(
            &store,
            &strict(&id, &reference, &pin, &store),
            InitialTaskReason::TaskMissing,
        );
    }
}

/// Root/self, ref, pin and coverage failures each return their own closed reason.
#[test]
fn identity_pin_and_coverage_failures_are_typed() {
    let home = common::Home::new();
    let mut store = home.store();
    let (id, reference) = root(&home, &mut store, "root task");
    let pin = pin(&store, &id);
    let base = strict(&id, &reference, &pin, &store);

    // Unknown ID, with and without a surviving pin tombstone for that ID.
    let ghost = AgentId::new();
    refused(&store, &current(&ghost), InitialTaskReason::NotFound);
    let mut request = base.clone();
    request.agent_id = ghost.clone();
    refused(&store, &request, InitialTaskReason::NotFound);
    let ghost_pin = pin_for(&store, &ghost);
    request.pin_id = Some(ghost_pin);
    refused(&store, &request, InitialTaskReason::RetentionDeleted);
    refused(&store, &current(&ghost), InitialTaskReason::NotFound);

    // Execution ref: different incarnation, and no first attempt at all.
    let other_ref = {
        let (other, other_ref) = root(&home, &mut store, "other");
        let _ = other;
        other_ref
    };
    let mut request = base.clone();
    request.execution_ref = Some(other_ref.clone());
    refused(&store, &request, InitialTaskReason::ExecutionRefMismatch);
    let mut fenced = current(&id);
    fenced.execution_ref = Some(other_ref);
    refused(&store, &fenced, InitialTaskReason::ExecutionRefMismatch);

    // Pin states.
    let mut request = base.clone();
    request.pin_id = Some(PinId::new());
    refused(&store, &request, InitialTaskReason::PinMissing);
    let (stranger, _) = root(&home, &mut store, "stranger");
    request.pin_id = Some(pin_for(&store, &stranger));
    refused(&store, &request, InitialTaskReason::PinMismatch);
    page(read(&store, &current(&id)));

    // Sequence other than the root's is not exportable even with correct evidence.
    let mut request = base.clone();
    request.sequence = Some(2);
    refused(&store, &request, InitialTaskReason::NotRootExecution);

    // Incomplete coverage: a retained child execution without verifiable provider
    // identity breaks the strict proof, while current mode makes no such claim.
    let child = {
        let mut request = home.request();
        request.task = "child".into();
        store
            .admit(&request, &home.config, &json!({}), None)
            .unwrap()
            .0
    };
    store
        .conn
        .execute(
            "UPDATE agents SET root_agent_id=?1,parent_agent_id=?1,sequence=2,status='succeeded' WHERE id=?2",
            params![id.as_str(), child.as_str()],
        )
        .unwrap();
    refused(&store, &base, InitialTaskReason::CoverageIncomplete);
    assert_eq!(page(read(&store, &current(&id))).text, "root task");
    // The later execution is an alias of the root lineage, never an exportable root.
    refused(
        &store,
        &current(&child),
        InitialTaskReason::NotRootExecution,
    );
    let mut request = base.clone();
    request.agent_id = child.clone();
    refused(&store, &request, InitialTaskReason::NotRootExecution);

    // Released pin: the tombstone remains but grants no retention.
    store
        .conn
        .execute(
            "UPDATE agents SET root_agent_id=id,parent_agent_id=NULL,sequence=1 WHERE id=?",
            [child.as_str()],
        )
        .unwrap();
    page(read(&store, &base));
    store
        .conn
        .execute(
            "UPDATE history_pins SET released=1 WHERE pin_id=?",
            [pin.as_str()],
        )
        .unwrap();
    refused(&store, &base, InitialTaskReason::PinReleased);
    assert_eq!(page(read(&store, &current(&id))).text, "root task");

    // No usable first attempt means no ref, in either mode.
    store
        .conn
        .execute(
            "UPDATE attempts SET id='att_not_a_uuid' WHERE agent_id=? AND number=1",
            [id.as_str()],
        )
        .unwrap();
    refused(
        &store,
        &current(&id),
        InitialTaskReason::ExecutionRefUnavailable,
    );
}

fn pin_for(store: &Store<'_>, root: &AgentId) -> PinId {
    pin(store, root)
}

/// Later pages fail closed if the text changed, and replay is exact otherwise.
#[test]
fn continuation_fence_detects_content_change() {
    let home = common::Home::new();
    let mut store = home.store();
    let task = "0123456789".repeat(10);
    let (id, reference) = root(&home, &mut store, &task);
    let pin = pin(&store, &id);
    let mut request = strict(&id, &reference, &pin, &store);
    request.max_bytes = 40;
    let first = page(read(&store, &request));
    assert_eq!(first.next_offset, Some(40));
    request.offset = 40;
    request.expected_sha256 = Some(first.sha256.clone());
    request.expected_total_bytes = Some(first.total_bytes);
    let second = page(read(&store, &request));
    assert_eq!(second.text, task[40..80]);
    assert_eq!(second.sha256, first.sha256);

    // Same length, different bytes: the SHA fence refuses.
    let altered = task.replacen('0', "X", 1);
    set_task(&store, &id, &altered);
    refused(&store, &request, InitialTaskReason::ContentChanged);
    // Same bytes, different length claim.
    set_task(&store, &id, &task);
    request.expected_total_bytes = Some(first.total_bytes + 1);
    refused(&store, &request, InitialTaskReason::ContentChanged);
    request.expected_total_bytes = Some(first.total_bytes);
    assert_eq!(page(read(&store, &request)).text, task[40..80]);
    // A shorter text cannot satisfy an offset beyond it.
    set_task(&store, &id, "short");
    refused(&store, &request, InitialTaskReason::ContentChanged);
}

/// Every page is one snapshot: concurrent change is invisible inside it.
#[test]
fn page_reads_from_one_snapshot() {
    let home = common::Home::new();
    let mut store = home.store();
    let (id, _) = root(&home, &mut store, "stable text");
    let mut reader = home.store();
    let writer = home.store();
    let tx = reader.conn.transaction().unwrap();
    let before = page(initial_task::read_in_snapshot(&tx, &current(&id)).unwrap());
    set_task(&writer, &id, "changed text");
    let during = page(initial_task::read_in_snapshot(&tx, &current(&id)).unwrap());
    assert_eq!(before, during);
    tx.commit().unwrap();
    assert_eq!(page(read(&reader, &current(&id))).text, "changed text");
}
