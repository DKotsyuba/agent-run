//! Authenticated pool chat, proposals, votes, budgets and derived status.

mod common;

/// Snapshot of agent rows, the pool roster revision and current member rows.
type ReplacementState = (
    Vec<(String, String, Option<f64>)>,
    u32,
    Vec<(String, String, Option<String>)>,
);

use agent_run_domain::pool::{
    CheckStatus, CriterionCheck, PoolDenial, PoolMessage, PoolPropose, PoolVote, VoteDecision,
};
use agent_run_domain::{
    catalog::{ProviderCatalog, QuotaCandidateSet, ResolvedLaunchAuthority, SelectionIntent},
    domain::AgentId,
    Error, HarnessId, ProviderConnection, ProviderStartRequest,
};
use agent_run_store::Store;
use agent_run_store::{
    pool_log::PoolWrite,
    pool_replace::{PoolReplaceInput, PoolReplacement},
    provider_admission::AdmissionInputs,
};
use serde_json::{json, Value};

/// Issues one worker capability and returns (run, attempt, token).
fn capability(store: &mut Store, id: &AgentId) -> (AgentId, String, String) {
    store
        .conn
        .execute(
            "UPDATE agents SET status='running',started_at=1.0 WHERE id=?",
            [id.as_str()],
        )
        .unwrap();
    store.event(id, "status", &json!({})).unwrap();
    let token = format!("{}{}", "a".repeat(32), "1".repeat(32));
    let attempt = format!(
        "att_{}{}",
        "b".repeat(20),
        id.as_str()[24..28].to_lowercase()
    );
    store
        .conn
        .execute(
            "INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,ownership_active) \
             VALUES(?,? ,1,'running','{}',1.0,1)",
            rusqlite::params![attempt, id.as_str()],
        )
        .unwrap();
    store
        .issue_worker_capability(id, &attempt, &token, 1.0)
        .unwrap();
    (id.clone(), attempt, token)
}

/// Admits one plain agent row for fixture use.
fn plain_agent(store: &mut Store, home: &common::Home) -> AgentId {
    store
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap()
        .0
}

/// Creates a two-member pool with one acceptance criterion and returns the
/// member execution rows with live worker capabilities.
fn pool(
    home: &common::Home,
    criteria: &[(&str, &str)],
) -> (String, Vec<(AgentId, String, String)>) {
    pool_with_names(home, criteria, ["member-1", "member-2"])
}

/// Creates a two-member pool with fixture-selected names and live capabilities.
fn pool_with_names(
    home: &common::Home,
    criteria: &[(&str, &str)],
    names: [&str; 2],
) -> (String, Vec<(AgentId, String, String)>) {
    let mut store = home.store();
    let a = plain_agent(&mut store, home);
    let b = plain_agent(&mut store, home);
    let acceptance: Vec<Value> = criteria
        .iter()
        .map(|(id, text)| json!({"id": id, "text": text}))
        .collect();
    store
        .conn
        .execute(
            "INSERT INTO pools(id,request_namespace,request_id,request_sha256,goal,acceptance_json,state,roster_revision,created_at) \
             VALUES('pool-20260101-000000-0123456789','ns','r1','0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef','ship it',?,'open',1,1.0)",
            [serde_json::to_string(&acceptance).unwrap()],
        )
        .unwrap();
    for (slot, id) in [(1, &a), (2, &b)] {
        store
            .conn
            .execute(
                "INSERT INTO pool_members(agent_id,pool_id,slot,name,role,personal_task,joined_roster_revision) \
                 VALUES(?, 'pool-20260101-000000-0123456789', ?, ?, 'doer', 'task', 1)",
                rusqlite::params![id.as_str(), slot, names[(slot - 1) as usize]],
            )
            .unwrap();
    }
    let first = capability(&mut store, &a);
    let second = capability(&mut store, &b);
    (
        "pool-20260101-000000-0123456789".to_owned(),
        vec![first, second],
    )
}

/// Tries one replacement with inert admission inputs; name collisions return first.
fn replacement_with_name(
    home: &common::Home,
    store: &mut Store,
    pool_id: &str,
    old: &AgentId,
    name: &str,
) -> agent_run_domain::Result<std::result::Result<PoolReplacement, PoolDenial>> {
    let catalog = ProviderCatalog::new(vec![], vec![]).unwrap();
    let request: ProviderStartRequest = serde_json::from_value(json!({
        "provider": "fixture", "model": "fixture", "profile": "review",
        "task": "fixture", "workdir": home.path,
    }))
    .unwrap();
    let effective = home.request();
    let authority = ResolvedLaunchAuthority {
        provider: "fixture".parse().unwrap(),
        harness: HarnessId::ClaudeCode,
        connection: ProviderConnection::Native,
        model: "fixture".into(),
        effort: None,
        profile: "review".into(),
        workdir: home.path.clone(),
        role_payload: json!({}),
        assets_sha256: "0".repeat(64).parse().unwrap(),
        eligible_accounts: vec![],
    };
    let candidates = QuotaCandidateSet {
        provider: "fixture".parse().unwrap(),
        model: "fixture".into(),
        intent: SelectionIntent::Auto,
        candidates: vec![],
        capacity_revision: 0,
    };
    let digest = "a".repeat(64);
    let identity = json!({});
    store.replace_pool_member(PoolReplaceInput {
        pool_id: &pool_id.parse().unwrap(),
        old,
        request_id: "name-check",
        request_sha256: &digest,
        expected_roster_revision: 1,
        new_id: "ag-20260101-000000-0000000009".parse().unwrap(),
        name,
        personal_task: "fixture",
        catalog: &catalog,
        inputs: AdmissionInputs {
            request: &request,
            effective: &effective,
            authority: &authority,
            candidates: &candidates,
            identity: &identity,
            global_cap: 5,
            harness_cap: None,
            pinned: None,
        },
    })
}

/// Captures agent state, roster revision, and member rows to prove a rejected attempt is atomic.
fn replacement_state(store: &Store, pool_id: &str) -> ReplacementState {
    let mut agents = store
        .conn
        .prepare("SELECT id,status,finished_at FROM agents ORDER BY id")
        .unwrap();
    let agents = agents
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    let revision = store
        .conn
        .query_row(
            "SELECT roster_revision FROM pools WHERE id=?",
            [pool_id],
            |row| row.get(0),
        )
        .unwrap();
    let members = store
        .conn
        .prepare("SELECT agent_id,name,replaced_by FROM pool_members WHERE pool_id=? ORDER BY slot")
        .unwrap()
        .query_map([pool_id], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    (agents, revision, members)
}

/// One member chat message.
fn message(key: &str, body: &str) -> PoolWrite {
    PoolWrite::Message(PoolMessage {
        request_id: key.into(),
        message: body.into(),
    })
}

/// One ready vote covering every criterion.
fn ready(key: &str, proposal: u64, criteria: &[(&str, &str)]) -> PoolWrite {
    PoolWrite::Vote(PoolVote {
        request_id: key.into(),
        proposal_seq: proposal,
        decision: VoteDecision::Ready,
        checks: criteria
            .iter()
            .map(|(id, _)| CriterionCheck {
                criterion_id: (*id).into(),
                status: CheckStatus::Met,
                evidence: format!("verified {id}"),
            })
            .collect(),
        message: None,
    })
}

/// Discovery orders creation timestamps and stable ids descending, filters before
/// counting/paging, bounds UTF-8 excerpts, and never exposes criteria or log bodies.
#[test]
fn list_pools_orders_filters_and_pages_without_writing() {
    use agent_run_domain::pool::{ListPoolsQuery, PoolState};
    let home = common::Home::new();
    let (pool, _) = pool(&home, &[("done", "verified")]);
    let store = home.store();
    for id in [
        "pool-20260102-000000-012345678a",
        "pool-20260102-000000-012345678b",
    ] {
        store.conn.execute(
            "INSERT INTO pools(id,request_namespace,request_id,request_sha256,goal,acceptance_json,state,roster_revision,created_at) \
             VALUES(?,'ns',? ,? ,? ,'[]','open',1,2.0)",
            rusqlite::params![id, id, "0".repeat(64), "漢".repeat(200)],
        ).unwrap();
    }
    store
        .conn
        .execute(
            "UPDATE pools SET state='completed',completed_at=3.0 WHERE id='pool-20260102-000000-012345678b'",
            [],
        )
        .unwrap();
    let revision = store.revision().unwrap();
    let query = ListPoolsQuery {
        limit: 1,
        ..Default::default()
    };
    let first = store.list_pools(&query).unwrap();
    assert_eq!(first.total, 3);
    assert_eq!(
        first.items[0].pool_id.as_str(),
        "pool-20260102-000000-012345678b"
    );
    assert_eq!(first.items[0].goal.len(), 510);
    assert!(first.items[0].goal_truncated);
    assert_eq!(first.next_offset, Some(1));
    assert!(!first.complete);
    let second = store
        .list_pools(&ListPoolsQuery {
            offset: 1,
            ..query.clone()
        })
        .unwrap();
    assert_eq!(
        second.items[0].pool_id.as_str(),
        "pool-20260102-000000-012345678a"
    );
    let last = store
        .list_pools(&ListPoolsQuery {
            offset: 2,
            ..query.clone()
        })
        .unwrap();
    assert_eq!(last.items[0].pool_id.as_str(), pool);
    assert!(last.complete);
    assert_eq!(last.next_offset, None);
    let beyond = store
        .list_pools(&ListPoolsQuery {
            offset: 99,
            ..query
        })
        .unwrap();
    assert_eq!(beyond.total, 3);
    assert!(beyond.items.is_empty() && beyond.complete);
    for (state, total) in [(PoolState::Open, 2), (PoolState::Completed, 1)] {
        let page = store
            .list_pools(&ListPoolsQuery {
                state: Some(state),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(page.total, total);
        assert!(page.items.iter().all(|item| item.state == state));
        let wire = serde_json::to_value(&page).unwrap();
        assert!(wire["items"][0].get("criteria").is_none());
        assert!(wire["items"][0].get("entries").is_none());
    }
    assert_eq!(store.revision().unwrap(), revision);
}

/// A raw ready decision never counts after failure, a new proposal, roster
/// revision or continuation invalidates it; list and pool use identical validity.
#[test]
fn list_pools_counts_only_valid_votes() {
    use agent_run_domain::pool::ListPoolsQuery;
    let home = common::Home::new();
    let (_, members, pool_id) = voted_pool(&home);
    let mut store = home.store();
    assert_eq!(
        store.list_pools(&ListPoolsQuery::default()).unwrap().items[0].ready,
        2
    );
    store
        .conn
        .execute(
            "UPDATE agents SET status='failed' WHERE id=?",
            [members[0].0.as_str()],
        )
        .unwrap();
    assert_eq!(
        store.list_pools(&ListPoolsQuery::default()).unwrap().items[0].ready,
        1
    );
    store
        .conn
        .execute(
            "UPDATE agents SET status='running' WHERE id=?",
            [members[0].0.as_str()],
        )
        .unwrap();
    let child = plain_agent_with_root(&mut store, &home, &members[0].0);
    store
        .conn
        .execute(
            "UPDATE agents SET status='running' WHERE id=?",
            [child.as_str()],
        )
        .unwrap();
    assert_eq!(
        store.list_pools(&ListPoolsQuery::default()).unwrap().items[0].ready,
        1
    );
    store
        .conn
        .execute(
            "UPDATE pools SET roster_revision=2 WHERE id=?",
            [pool_id.as_str()],
        )
        .unwrap();
    let page = store.list_pools(&ListPoolsQuery::default()).unwrap();
    assert_eq!(page.items[0].ready, 0);
    let status = store
        .pool_operator_read(&pool_id, 0, None, 50)
        .unwrap()
        .unwrap();
    assert_eq!(
        page.items[0].ready,
        status["status"]["members"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|member| member["counts"] == true)
            .count()
    );
    let (run, attempt, token) = &members[1];
    let new = store
        .pool_write(
            run,
            attempt,
            token,
            PoolWrite::Proposal(PoolPropose {
                request_id: "new-proposal".into(),
                message: "New result".into(),
                snapshot: "New snapshot".into(),
            }),
        )
        .unwrap()
        .unwrap();
    let page = store.list_pools(&ListPoolsQuery::default()).unwrap();
    assert_eq!(page.items[0].current_proposal_seq, Some(new.seq));
    assert_eq!(page.items[0].last_seq, new.seq);
    assert_eq!(page.items[0].ready, 0);
}

/// Unbound member chat works: membership, not a binding, authorizes writes.
#[test]
fn unbound_member_chat_is_recorded_with_stamped_author() {
    let home = common::Home::new();
    let (_, members) = pool(&home, &[("done", "it ships")]);
    let (run, attempt, token) = &members[0];
    let mut store = home.store();
    let receipt = store
        .pool_write(run, attempt, token, message("m1", "hello team"))
        .unwrap()
        .unwrap();
    assert_eq!(receipt.seq, 1);
    assert!(!receipt.duplicate);
    let author: (String, String, String, String) = store
        .conn
        .query_row(
            "SELECT author_kind,author_agent_id,author_name,author_role FROM pool_entries WHERE seq=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(
        author,
        (
            "member".into(),
            run.as_str().into(),
            "member-1".into(),
            "doer".into()
        )
    );
}

/// The same key with the same content replays; different content conflicts.
#[test]
fn same_key_replays_and_changed_content_conflicts() {
    let home = common::Home::new();
    let (_, members) = pool(&home, &[("done", "it ships")]);
    let (run, attempt, token) = &members[0];
    let mut store = home.store();
    let first = store
        .pool_write(run, attempt, token, message("k", "one"))
        .unwrap()
        .unwrap();
    let second = store
        .pool_write(run, attempt, token, message("k", "one"))
        .unwrap()
        .unwrap();
    assert_eq!(first.seq, second.seq);
    assert!(second.duplicate);
    let changed = store
        .pool_write(run, attempt, token, message("k", "two"))
        .unwrap()
        .unwrap_err();
    assert_eq!(changed, PoolDenial::Conflict);
    let count: i64 = store
        .conn
        .query_row("SELECT COUNT(*) FROM pool_entries", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1);
}

/// Invalid tokens, non-members, terminal attempts and completed pools are
/// each refused; the author is never taken from the caller.
#[test]
fn authentication_and_membership_gate_every_write() {
    let home = common::Home::new();
    let (_, members) = pool(&home, &[("done", "it ships")]);
    let (run, attempt, token) = &members[0];
    let outsider = {
        let mut store = home.store();
        let outsider = plain_agent(&mut store, &home);
        capability(&mut store, &outsider)
    };
    let mut store = home.store();
    // Wrong token shape/hash.
    assert!(store
        .pool_write(run, attempt, &"0".repeat(64), message("a", "x"))
        .is_err());
    // A live non-member with a valid capability of its own.
    let denial = store
        .pool_write(&outsider.0, &outsider.1, &outsider.2, message("b", "x"))
        .unwrap()
        .unwrap_err();
    assert_eq!(denial, PoolDenial::NotPoolMember);
    // A replaced member loses its seat.
    let replacement = plain_agent(&mut store, &home);
    store
        .conn
        .execute(
            "INSERT INTO pool_members(agent_id,pool_id,slot,name,role,personal_task,joined_roster_revision)              VALUES(?,'pool-20260101-000000-0123456789',3,'member-3','doer','task',2)",
            [replacement.as_str()],
        )
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE pool_members SET replaced_by=? WHERE agent_id=?",
            rusqlite::params![replacement.as_str(), run.as_str()],
        )
        .unwrap();
    assert_eq!(
        store
            .pool_write(run, attempt, token, message("c", "x"))
            .unwrap()
            .unwrap_err(),
        PoolDenial::NotPoolMember
    );
    // A terminal attempt no longer authenticates.
    let (run2, attempt2, token2) = &members[1];
    store
        .conn
        .execute(
            "UPDATE agents SET status='succeeded',finished_at=9.0 WHERE id=?",
            [run2.as_str()],
        )
        .unwrap();
    assert!(store
        .pool_write(run2, attempt2, token2, message("d", "x"))
        .is_err());
}

/// Votes need the current proposal; ready needs exact criterion coverage;
/// the derived status explains every member and never completes the pool.
#[test]
fn proposals_votes_and_derived_status() {
    let criteria = &[("done", "it ships"), ("tested", "tests pass")];
    let home = common::Home::new();
    let (_, members) = pool(&home, criteria);
    let (run, attempt, token) = &members[0];
    let (run2, attempt2, token2) = &members[1];
    let mut store = home.store();
    // No proposal yet.
    assert_eq!(
        store
            .pool_write(run, attempt, token, ready("v0", 1, criteria))
            .unwrap()
            .unwrap_err(),
        PoolDenial::StaleProposal { current: None }
    );
    // Propose.
    let proposal = store
        .pool_write(
            run,
            attempt,
            token,
            PoolWrite::Proposal(PoolPropose {
                request_id: "p1".into(),
                message: "result ready".into(),
                snapshot: "commit abc123".into(),
            }),
        )
        .unwrap()
        .unwrap();
    assert_eq!(proposal.seq, 1);
    // Voting on a stale proposal sequence refuses with the current one.
    assert_eq!(
        store
            .pool_write(run, attempt, token, ready("v1", 99, criteria))
            .unwrap()
            .unwrap_err(),
        PoolDenial::StaleProposal {
            current: Some(proposal.seq)
        }
    );
    // Incomplete coverage refuses.
    assert_eq!(
        store
            .pool_write(
                run,
                attempt,
                token,
                PoolWrite::Vote(PoolVote {
                    request_id: "v2".into(),
                    proposal_seq: proposal.seq,
                    decision: VoteDecision::Ready,
                    checks: vec![CriterionCheck {
                        criterion_id: "done".into(),
                        status: CheckStatus::Met,
                        evidence: "ships".into(),
                    }],
                    message: None,
                })
            )
            .unwrap()
            .unwrap_err(),
        PoolDenial::MalformedChecks
    );
    // One member votes ready; the other is missing. Agreement is false and
    // the state stays open: agreement while running is not completion.
    store
        .pool_write(run, attempt, token, ready("v3", proposal.seq, criteria))
        .unwrap()
        .unwrap();
    let page = store
        .pool_read(run, attempt, token, 0, None, 50)
        .unwrap()
        .unwrap();
    assert_eq!(page["status"]["agreed"], false);
    assert_eq!(page["status"]["state"], "open");
    let reasons: Vec<&str> = page["status"]["members"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["why"].as_str().unwrap())
        .collect();
    assert_eq!(reasons, ["valid", "missing"]);
    // The second member's ready vote makes agreement true while both run;
    // still no completion writer fires.
    store
        .pool_write(run2, attempt2, token2, ready("w1", proposal.seq, criteria))
        .unwrap()
        .unwrap();
    let page = store
        .pool_read(run, attempt, token, 0, None, 50)
        .unwrap()
        .unwrap();
    assert_eq!(page["status"]["agreed"], true);
    assert_eq!(page["status"]["state"], "open");
    // Informational chat keeps votes valid.
    store
        .pool_write(run, attempt, token, message("c1", "fyi"))
        .unwrap()
        .unwrap();
    let page = store
        .pool_read(run, attempt, token, 0, None, 50)
        .unwrap()
        .unwrap();
    assert_eq!(page["status"]["agreed"], true);
    // A block always remains possible and flips agreement off.
    store
        .pool_write(
            run2,
            attempt2,
            token2,
            PoolWrite::Vote(PoolVote {
                request_id: "w2".into(),
                proposal_seq: proposal.seq,
                decision: VoteDecision::Block,
                checks: vec![],
                message: Some("regression found".into()),
            }),
        )
        .unwrap()
        .unwrap();
    let page = store
        .pool_read(run, attempt, token, 0, None, 50)
        .unwrap()
        .unwrap();
    assert_eq!(page["status"]["agreed"], false);
    assert_eq!(page["status"]["members"][1]["why"], "blocked");
    // A new proposal invalidates the earlier votes.
    let proposal2 = store
        .pool_write(
            run,
            attempt,
            token,
            PoolWrite::Proposal(PoolPropose {
                request_id: "p2".into(),
                message: "fixed".into(),
                snapshot: "commit def456".into(),
            }),
        )
        .unwrap()
        .unwrap();
    let page = store
        .pool_read(run, attempt, token, 0, None, 50)
        .unwrap()
        .unwrap();
    assert_eq!(page["status"]["agreed"], false);
    assert_eq!(page["status"]["members"][0]["why"], "missing");
    // A resume (a newer execution in the lineage) makes the cast vote stale.
    store
        .pool_write(run, attempt, token, ready("v4", proposal2.seq, criteria))
        .unwrap()
        .unwrap();
    let resumed = {
        let mut store = home.store();
        plain_agent_with_root(&mut store, &home, run)
    };
    let (resumed_run, resumed_attempt, resumed_token) = capability(&mut store, &resumed);
    let page = store
        .pool_read(&resumed_run, &resumed_attempt, &resumed_token, 0, None, 50)
        .unwrap()
        .unwrap();
    assert_eq!(page["status"]["members"][0]["why"], "stale_tip");
    assert_eq!(page["status"]["members"][0]["counts"], false);
}

/// Admits a child execution continuing `root`'s lineage.
fn plain_agent_with_root(store: &mut Store, home: &common::Home, root: &AgentId) -> AgentId {
    let id = plain_agent(store, home);
    store
        .conn
        .execute(
            "UPDATE agents SET parent_agent_id=?,root_agent_id=?,sequence=2 WHERE id=?",
            rusqlite::params![root.as_str(), root.as_str(), id.as_str()],
        )
        .unwrap();
    id
}

/// Ordinary chat hits its row budget; block, revoke and votes stay possible.
#[test]
fn chat_budget_never_blocks_control_writes() {
    let home = common::Home::new();
    let (_, members) = pool(&home, &[("done", "it ships")]);
    let (run, attempt, token) = &members[0];
    let (run2, attempt2, token2) = &members[1];
    let mut store = home.store();
    let proposal = store
        .pool_write(
            run,
            attempt,
            token,
            PoolWrite::Proposal(PoolPropose {
                request_id: "p".into(),
                message: "ready".into(),
                snapshot: "snap".into(),
            }),
        )
        .unwrap()
        .unwrap();
    for index in 0..agent_run_store::pool_log::CHAT_ROW_BUDGET {
        store
            .pool_write(run, attempt, token, message(&format!("c{index}"), "x"))
            .unwrap()
            .unwrap();
    }
    assert_eq!(
        store
            .pool_write(run, attempt, token, message("over", "x"))
            .unwrap()
            .unwrap_err(),
        PoolDenial::ChatBudgetExhausted
    );
    store
        .pool_write(
            run,
            attempt,
            token,
            PoolWrite::Vote(PoolVote {
                request_id: "v".into(),
                proposal_seq: proposal.seq,
                decision: VoteDecision::Ready,
                checks: vec![CriterionCheck {
                    criterion_id: "done".into(),
                    status: CheckStatus::Met,
                    evidence: "yes".into(),
                }],
                message: None,
            }),
        )
        .unwrap()
        .unwrap();
    store
        .pool_write(
            run2,
            attempt2,
            token2,
            PoolWrite::Vote(PoolVote {
                request_id: "b".into(),
                proposal_seq: proposal.seq,
                decision: VoteDecision::Block,
                checks: vec![],
                message: None,
            }),
        )
        .unwrap()
        .unwrap();
    store
        .pool_write(
            run2,
            attempt2,
            token2,
            PoolWrite::Vote(PoolVote {
                request_id: "r".into(),
                proposal_seq: proposal.seq,
                decision: VoteDecision::Revoke,
                checks: vec![],
                message: None,
            }),
        )
        .unwrap()
        .unwrap();
}

/// The per-member vote budget bounds repeated voting and reserves the last
/// slot for a block.
#[test]
fn vote_budget_reserves_a_block_slot() {
    let home = common::Home::new();
    let (_, members) = pool(&home, &[("done", "it ships")]);
    let (run, attempt, token) = &members[0];
    let mut store = home.store();
    let proposal = store
        .pool_write(
            run,
            attempt,
            token,
            PoolWrite::Proposal(PoolPropose {
                request_id: "p".into(),
                message: "m".into(),
                snapshot: "s".into(),
            }),
        )
        .unwrap()
        .unwrap();
    for index in 0..agent_run_store::pool_log::VOTE_ROW_BUDGET - 1 {
        store
            .pool_write(
                run,
                attempt,
                token,
                PoolWrite::Vote(PoolVote {
                    request_id: format!("v{index}"),
                    proposal_seq: proposal.seq,
                    decision: VoteDecision::Ready,
                    checks: vec![CriterionCheck {
                        criterion_id: "done".into(),
                        status: CheckStatus::Met,
                        evidence: "ok".into(),
                    }],
                    message: None,
                }),
            )
            .unwrap()
            .unwrap();
    }
    // The last slot refuses another ready vote…
    assert_eq!(
        store
            .pool_write(
                run,
                attempt,
                token,
                PoolWrite::Vote(PoolVote {
                    request_id: "vr".into(),
                    proposal_seq: proposal.seq,
                    decision: VoteDecision::Ready,
                    checks: vec![CriterionCheck {
                        criterion_id: "done".into(),
                        status: CheckStatus::Met,
                        evidence: "ok".into(),
                    }],
                    message: None,
                })
            )
            .unwrap()
            .unwrap_err(),
        PoolDenial::VoteBudgetExhausted
    );
    // …but a block always fits.
    store
        .pool_write(
            run,
            attempt,
            token,
            PoolWrite::Vote(PoolVote {
                request_id: "vb".into(),
                proposal_seq: proposal.seq,
                decision: VoteDecision::Block,
                checks: vec![],
                message: None,
            }),
        )
        .unwrap()
        .unwrap();
    // Beyond the bound even a block is bounded.
    assert_eq!(
        store
            .pool_write(
                run,
                attempt,
                token,
                PoolWrite::Vote(PoolVote {
                    request_id: "vb2".into(),
                    proposal_seq: proposal.seq,
                    decision: VoteDecision::Block,
                    checks: vec![],
                    message: None,
                })
            )
            .unwrap()
            .unwrap_err(),
        PoolDenial::VoteBudgetExhausted
    );
}

/// Aggregate escaped check evidence is rejected before a vote or event is stored.
#[test]
fn oversized_vote_checks_are_typed_validation_without_rows() {
    let home = common::Home::new();
    let (_, members) = pool(&home, &[("done", "it ships")]);
    let (run, attempt, token) = &members[0];
    let vote = PoolWrite::Vote(PoolVote {
        request_id: "large-vote".into(),
        proposal_seq: 1,
        decision: VoteDecision::Block,
        checks: (0..10)
            .map(|index| CriterionCheck {
                criterion_id: format!("c{index}"),
                status: CheckStatus::Met,
                evidence: "\"".repeat(1024),
            })
            .collect(),
        message: None,
    });
    let mut store = home.store();
    assert!(matches!(
        store.pool_write(run, attempt, token, vote),
        Err(Error::Validation(_))
    ));
    let (votes, events): (i64, i64) = store
        .conn
        .query_row(
            "SELECT (SELECT COUNT(*) FROM pool_entries WHERE kind='vote'), \
                (SELECT COUNT(*) FROM events WHERE kind='pool_entry_appended')",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!((votes, events), (0, 0));
}

/// Reads page by the immutable cursor in both directions and report no
/// internal execution identity.
#[test]
fn read_pages_by_cursor_without_internal_ids() {
    let home = common::Home::new();
    let (_, members) = pool(&home, &[("done", "it ships")]);
    let (run, attempt, token) = &members[0];
    let mut store = home.store();
    for index in 1..=7 {
        store
            .pool_write(
                run,
                attempt,
                token,
                message(&format!("k{index}"), &format!("m{index}")),
            )
            .unwrap()
            .unwrap();
    }
    let first = store
        .pool_read(run, attempt, token, 0, None, 2)
        .unwrap()
        .unwrap();
    assert_eq!(first["entries"].as_array().unwrap().len(), 2);
    assert_eq!(first["complete"], false);
    let next = first["next_cursor"].as_i64().unwrap() as u64;
    let rest = store
        .pool_read(run, attempt, token, next, None, 50)
        .unwrap()
        .unwrap();
    assert_eq!(rest["complete"], true);
    assert_eq!(rest["entries"].as_array().unwrap().len(), 5);
    let forward_seqs: Vec<u64> = first["entries"]
        .as_array()
        .unwrap()
        .iter()
        .chain(rest["entries"].as_array().unwrap())
        .map(|entry| entry["seq"].as_u64().unwrap())
        .collect();
    assert_eq!(forward_seqs, (1..=7).collect::<Vec<_>>());

    let mut before = Some(8);
    let mut reverse_pages = Vec::new();
    loop {
        let page = store
            .pool_read(run, attempt, token, 0, before, 2)
            .unwrap()
            .unwrap();
        reverse_pages.extend(
            page["entries"]
                .as_array()
                .unwrap()
                .iter()
                .map(|entry| entry["seq"].as_u64().unwrap()),
        );
        if page["complete"] == true {
            break;
        }
        before = Some(page["next_cursor"].as_u64().unwrap());
    }
    assert_eq!(reverse_pages, [6, 7, 4, 5, 2, 3, 1]);
    let mut reverse_sorted = reverse_pages.clone();
    reverse_sorted.sort_unstable();
    assert_eq!(reverse_sorted, (1..=7).collect::<Vec<_>>());
    let text = rest.to_string();
    assert!(!text.contains(&attempt.clone()), "no attempt ids leak");
}

/// Replacement names follow Rust's Unicode lowercase rule inside the transaction.
#[test]
fn replacement_name_collisions_are_unicode_safe_and_atomic() {
    let home = common::Home::new();
    let (pool_id, members) = pool_with_names(&home, &[("done", "it ships")], ["Old", "Äda"]);
    let old = &members[0].0;
    let mut store = home.store();
    for (run, attempt, _) in &members {
        store
            .conn
            .execute(
                "UPDATE agents SET status='failed',finished_at=2 WHERE id=?",
                [run.as_str()],
            )
            .unwrap();
        store.conn.execute(
            "UPDATE attempts SET state='failed',finished_at=2,phase='cleanup_complete',cleanup_proof_json=? WHERE id=?",
            rusqlite::params![r#"{"confirmed":true}"#, attempt],
        ).unwrap();
    }

    let before_unicode = replacement_state(&store, &pool_id);
    assert!(matches!(
        replacement_with_name(&home, &mut store, &pool_id, old, "äDA"),
        Err(Error::Validation(message)) if message == "member names must be unique"
    ));
    assert_eq!(replacement_state(&store, &pool_id), before_unicode);

    let before_reuse = replacement_state(&store, &pool_id);
    assert!(!matches!(
        replacement_with_name(&home, &mut store, &pool_id, old, "oLD"),
        Err(Error::Validation(message)) if message == "member names must be unique"
    ));
    assert_eq!(replacement_state(&store, &pool_id), before_reuse);

    let ascii_home = common::Home::new();
    let (ascii_pool, ascii_members) =
        pool_with_names(&ascii_home, &[("done", "it ships")], ["Old", "Peer"]);
    let ascii_old = &ascii_members[0].0;
    let mut ascii_store = ascii_home.store();
    for (run, attempt, _) in &ascii_members {
        ascii_store
            .conn
            .execute(
                "UPDATE agents SET status='failed',finished_at=2 WHERE id=?",
                [run.as_str()],
            )
            .unwrap();
        ascii_store.conn.execute(
            "UPDATE attempts SET state='failed',finished_at=2,phase='cleanup_complete',cleanup_proof_json=? WHERE id=?",
            rusqlite::params![r#"{"confirmed":true}"#, attempt],
        ).unwrap();
    }
    let before_ascii = replacement_state(&ascii_store, &ascii_pool);
    assert!(matches!(
        replacement_with_name(&ascii_home, &mut ascii_store, &ascii_pool, ascii_old, "pEER"),
        Err(Error::Validation(message)) if message == "member names must be unique"
    ));
    assert_eq!(replacement_state(&ascii_store, &ascii_pool), before_ascii);
}

/// A finished (terminal) attempt cannot read or write the log.
#[test]
fn terminal_attempt_cannot_read_or_write() {
    let home = common::Home::new();
    let (_, members) = pool(&home, &[("done", "it ships")]);
    let (run, attempt, token) = &members[0];
    home.store()
        .conn
        .execute(
            "UPDATE agents SET status='failed',finished_at=9.0 WHERE id=?",
            [run.as_str()],
        )
        .unwrap();
    let mut store = home.store();
    assert!(store
        .pool_write(run, attempt, token, message("x", "y"))
        .is_err());
    assert!(store.pool_read(run, attempt, token, 0, None, 5).is_err());
}

/// The fixed catalog's five names decode and unknown names refuse, matching
/// the worker surface that routes them.
#[test]
fn worker_tool_names_decode_exactly() {
    use agent_run_domain::worker::WorkerTool;
    assert_eq!(
        WorkerTool::parse("notify_orchestrator"),
        Some(WorkerTool::Notify)
    );
    assert_eq!(WorkerTool::parse("pool_post"), Some(WorkerTool::PoolPost));
    assert_eq!(WorkerTool::parse("pool_read"), Some(WorkerTool::PoolRead));
    assert_eq!(
        WorkerTool::parse("pool_propose"),
        Some(WorkerTool::PoolPropose)
    );
    assert_eq!(WorkerTool::parse("pool_vote"), Some(WorkerTool::PoolVote));
    for unknown in ["start", "pool", "steer", ""] {
        assert_eq!(WorkerTool::parse(unknown), None);
    }
}

/// The read wait probe reports entries beyond a cursor and typed refusals
/// for non-members, without holding any lock between probes.
#[test]
fn wait_probe_reports_entries_and_refuses_non_members() {
    let home = common::Home::new();
    let (_, members) = pool(&home, &[("done", "it ships")]);
    let (run, attempt, token) = &members[0];
    let outsider = {
        let mut store = home.store();
        let outsider = plain_agent(&mut store, &home);
        capability(&mut store, &outsider)
    };
    let mut store = home.store();
    assert_eq!(
        store
            .pool_has_entries_after(run, attempt, token, 0)
            .unwrap(),
        Ok(false)
    );
    store
        .pool_write(run, attempt, token, message("m", "hello"))
        .unwrap()
        .unwrap();
    assert_eq!(
        store
            .pool_has_entries_after(run, attempt, token, 0)
            .unwrap(),
        Ok(true)
    );
    assert_eq!(
        store
            .pool_has_entries_after(&outsider.0, &outsider.1, &outsider.2, 0)
            .unwrap(),
        Err(PoolDenial::NotPoolMemberRead)
    );
}

/// The byte budget is prospective: an ordinary message that would cross the
/// cap is refused without a new row, while control writes still land.
#[test]
fn byte_budget_refuses_oversized_addition_without_a_row() {
    let home = common::Home::new();
    let (_, members) = pool(&home, &[("done", "it ships")]);
    let (run, attempt, token) = &members[0];
    let mut store = home.store();
    let proposal = store
        .pool_write(
            run,
            attempt,
            token,
            PoolWrite::Proposal(PoolPropose {
                request_id: "p".into(),
                message: "m".into(),
                snapshot: "s".into(),
            }),
        )
        .unwrap()
        .unwrap();
    // Fill the chat bytes to just below the cap with valid rows.
    let big = "x".repeat(8192);
    let mut filled = 0usize;
    while filled + big.len() <= agent_run_store::pool_log::CHAT_BYTE_BUDGET as usize {
        store
            .pool_write(run, attempt, token, message(&format!("c{filled}"), &big))
            .unwrap()
            .unwrap();
        filled += big.len();
    }
    let remaining = agent_run_store::pool_log::CHAT_BYTE_BUDGET as usize - filled;
    assert!(remaining < big.len(), "the fixture sits below the byte cap");
    let rows_before: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM pool_entries WHERE kind='message'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    // An addition that would cross the cap is refused and writes no row.
    assert_eq!(
        store
            .pool_write(run, attempt, token, message("over", &big))
            .unwrap()
            .unwrap_err(),
        PoolDenial::ChatBudgetExhausted
    );
    let rows_after: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM pool_entries WHERE kind='message'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(rows_before, rows_after, "no row was written");
    // A control write still lands on the same pool.
    store
        .pool_write(
            run,
            attempt,
            token,
            PoolWrite::Vote(PoolVote {
                request_id: "b".into(),
                proposal_seq: proposal.seq,
                decision: VoteDecision::Block,
                checks: vec![],
                message: None,
            }),
        )
        .unwrap()
        .unwrap();
}

/// Late pool binding recovers a fast member failure and keeps individual success suppressed.
#[test]
fn stability_pool_late_bind_preserves_success_suppression() {
    let home = common::Home::new();
    let (pool, members) = pool(&home, &[("done", "done")]);
    let mut store = home.store();
    store
        .finish(
            &members[0].0,
            &agent_run_domain::domain::Outcome::failure("prepare_failed"),
            None,
            None,
        )
        .unwrap();
    let answer_dir = home.path.join("agents").join(members[1].0.as_str());
    agent_run_platform::fs::private_dir(&answer_dir).unwrap();
    let proof = agent_run_platform::verify::seal(
        &answer_dir,
        std::path::Path::new("answer.md"),
        "fixture result",
    )
    .unwrap();
    store
        .finish(
            &members[1].0,
            &agent_run_domain::domain::Outcome::success(None),
            Some(&proof),
            None,
        )
        .unwrap();
    let reference = agent_run_domain::domain::OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "late".into(),
        external_turn_id: None,
    };
    let pool = pool.parse().unwrap();
    store
        .bind_pool(&pool, &reference, agent_run_domain::domain::now())
        .unwrap();
    store
        .bind_pool(&pool, &reference, agent_run_domain::domain::now())
        .unwrap();
    let count: i64 = store
        .conn
        .query_row("SELECT COUNT(*) FROM deliveries", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1);
    assert_eq!(
        store.delivery_status(&members[1].0).unwrap()["state"],
        "not_created"
    );
}

/// Revoke and check-less block replay their exact stored form; changed notes conflict.
#[test]
fn stability_vote_replay_and_revoke_validation() {
    let home = common::Home::new();
    let (_pool, members) = pool(&home, &[("done", "done")]);
    let (run, attempt, token) = &members[0];
    let mut store = home.store();
    let proposal = store
        .pool_write(
            run,
            attempt,
            token,
            PoolWrite::Proposal(PoolPropose {
                request_id: "proposal".into(),
                message: "result".into(),
                snapshot: "result".into(),
            }),
        )
        .unwrap()
        .unwrap();
    for (key, decision) in [
        ("revoke", VoteDecision::Revoke),
        ("block", VoteDecision::Block),
    ] {
        let vote = PoolVote {
            request_id: key.into(),
            proposal_seq: proposal.seq,
            decision,
            checks: vec![],
            message: Some("note".into()),
        };
        let first = store
            .pool_write(run, attempt, token, PoolWrite::Vote(vote.clone()))
            .unwrap()
            .unwrap();
        let retry = store
            .pool_write(run, attempt, token, PoolWrite::Vote(vote.clone()))
            .unwrap()
            .unwrap();
        assert_eq!(first.seq, retry.seq);
        assert!(retry.duplicate);
        let changed = PoolVote {
            message: Some("changed".into()),
            ..vote
        };
        assert_eq!(
            store
                .pool_write(run, attempt, token, PoolWrite::Vote(changed))
                .unwrap(),
            Err(PoolDenial::Conflict)
        );
    }
    let invalid = PoolVote {
        request_id: "invalid".into(),
        proposal_seq: proposal.seq,
        decision: VoteDecision::Revoke,
        checks: vec![CriterionCheck {
            criterion_id: "done".into(),
            status: CheckStatus::Met,
            evidence: "checked".into(),
        }],
        message: None,
    };
    assert!(invalid.validate().is_err());
}

/// The final vote-slot reservation accepts a revoke as well as a block: a
/// member with seven rows can withdraw its valid ready vote, leaving a
/// non-ready status and never a completion.
#[test]
fn final_vote_slot_accepts_revoke_after_seven_votes() {
    let home = common::Home::new();
    let (_, members) = pool(&home, &[("done", "it ships")]);
    let (run, attempt, token) = &members[0];
    let mut store = home.store();
    let proposal = store
        .pool_write(
            run,
            attempt,
            token,
            PoolWrite::Proposal(PoolPropose {
                request_id: "p".into(),
                message: "m".into(),
                snapshot: "s".into(),
            }),
        )
        .unwrap()
        .unwrap();
    for index in 0..agent_run_store::pool_log::VOTE_ROW_BUDGET - 1 {
        store
            .pool_write(
                run,
                attempt,
                token,
                PoolWrite::Vote(PoolVote {
                    request_id: format!("v{index}"),
                    proposal_seq: proposal.seq,
                    decision: VoteDecision::Ready,
                    checks: vec![CriterionCheck {
                        criterion_id: "done".into(),
                        status: CheckStatus::Met,
                        evidence: "ok".into(),
                    }],
                    message: None,
                }),
            )
            .unwrap()
            .unwrap();
    }
    // A ready vote no longer fits the reserved slot…
    assert_eq!(
        store
            .pool_write(
                run,
                attempt,
                token,
                PoolWrite::Vote(PoolVote {
                    request_id: "vr".into(),
                    proposal_seq: proposal.seq,
                    decision: VoteDecision::Ready,
                    checks: vec![CriterionCheck {
                        criterion_id: "done".into(),
                        status: CheckStatus::Met,
                        evidence: "ok".into(),
                    }],
                    message: None,
                })
            )
            .unwrap()
            .unwrap_err(),
        PoolDenial::VoteBudgetExhausted
    );
    // …but a revoke does, withdrawing the member's vote.
    store
        .pool_write(
            run,
            attempt,
            token,
            PoolWrite::Vote(PoolVote {
                request_id: "w".into(),
                proposal_seq: proposal.seq,
                decision: VoteDecision::Revoke,
                checks: vec![],
                message: None,
            }),
        )
        .unwrap()
        .unwrap();
    let page = store
        .pool_read(run, attempt, token, 0, None, 50)
        .unwrap()
        .unwrap();
    assert_eq!(page["status"]["members"][0]["why"], "revoked");
    assert_eq!(page["status"]["members"][0]["counts"], false);
    assert_eq!(page["status"]["agreed"], false);
    assert_eq!(page["status"]["state"], "open", "no completion ever fires");
    // The budget stays finite: nothing more fits, not even another revoke.
    assert_eq!(
        store
            .pool_write(
                run,
                attempt,
                token,
                PoolWrite::Vote(PoolVote {
                    request_id: "w2".into(),
                    proposal_seq: proposal.seq,
                    decision: VoteDecision::Revoke,
                    checks: vec![],
                    message: None,
                })
            )
            .unwrap()
            .unwrap_err(),
        PoolDenial::VoteBudgetExhausted
    );
}

/// A member write fans out one pending `pool` command holding only the
/// sequence to every current peer tip — never to the sender — and a replay
/// of the same key enqueues nothing twice.
#[test]
fn write_fans_out_to_peer_tips_once() {
    let home = common::Home::new();
    let (_, members) = pool(&home, &[("done", "it ships")]);
    let (run, attempt, token) = &members[0];
    let (run2, _, _) = &members[1];
    let mut store = home.store();
    let receipt = store
        .pool_write(run, attempt, token, message("f1", "hi"))
        .unwrap()
        .unwrap();
    let peer_commands: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM commands WHERE agent_id=? AND kind='pool'",
            [run2.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(peer_commands, 1);
    let sender_commands: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM commands WHERE agent_id=? AND kind='pool'",
            [run.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(sender_commands, 0, "the sender is never pushed to itself");
    let payload: serde_json::Value = store
        .conn
        .query_row(
            "SELECT payload_json FROM commands WHERE agent_id=? AND kind='pool'",
            [run2.as_str()],
            |row| row.get::<_, String>(0),
        )
        .map(|raw| serde_json::from_str(&raw).unwrap())
        .unwrap();
    assert_eq!(payload, serde_json::json!({"seq": receipt.seq}));
    // Replay: the entry duplicates and no second command appears.
    let again = store
        .pool_write(run, attempt, token, message("f1", "hi"))
        .unwrap()
        .unwrap();
    assert!(again.duplicate);
    let peer_commands: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM commands WHERE agent_id=? AND kind='pool'",
            [run2.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(peer_commands, 1, "a replay enqueues nothing twice");
}

/// A pool member's notify creates the original worker notification plus one
/// linked team copy and peer commands in the same transaction, with no self
/// push; the non-member notify path is untouched.
#[test]
fn notify_copies_to_the_team_once_in_one_transaction() {
    let home = common::Home::new();
    let (_, members) = pool(&home, &[("done", "it ships")]);
    let (run, attempt, token) = &members[0];
    let (run2, _, _) = &members[1];
    // The notify route requires a bound session on a delivery transport.
    home.store()
        .conn
        .execute(
            "INSERT INTO orchestrator_sessions(id,transport,external_session_id,created_at,last_seen_at) \
             VALUES('os-1','claude_uds','sess-1',1.0,1.0)",
            [],
        )
        .unwrap();
    home.store()
        .conn
        .execute(
            "UPDATE agents SET orchestrator_session_id='os-1' WHERE id=?",
            [run.as_str()],
        )
        .unwrap();
    let mut store = home.store();
    let receipt = store
        .notify_orchestrator(
            run,
            attempt,
            token,
            &agent_run_domain::worker::NotifyRequest {
                request_id: "n1".into(),
                kind: agent_run_domain::worker::WorkerMessageKind::Risk,
                message: "material finding".into(),
            },
            2.0,
        )
        .unwrap();
    assert!(!receipt.duplicate);
    let copy: (String, String, String, String, String, String) = store
        .conn
        .query_row(
            "SELECT author_kind,direction,kind,severity,body,delivery_id FROM pool_entries \
             WHERE idem_scope=? AND request_id='n1'",
            rusqlite::params![format!("notify:{}", run.as_str())],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(
        copy,
        (
            "member".into(),
            "orchestrator_copy".into(),
            "report".into(),
            "risk".into(),
            "material finding".into(),
            receipt.notification_id.clone(),
        )
    );
    let peer_commands: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM commands WHERE agent_id=? AND kind='pool'",
            [run2.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(peer_commands, 1);
    let self_commands: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM commands WHERE agent_id=? AND kind='pool'",
            [run.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(self_commands, 0);
    // A retry with the same key replays the original receipt and adds
    // neither a copy nor a command.
    let replay = store
        .notify_orchestrator(
            run,
            attempt,
            token,
            &agent_run_domain::worker::NotifyRequest {
                request_id: "n1".into(),
                kind: agent_run_domain::worker::WorkerMessageKind::Risk,
                message: "material finding".into(),
            },
            40.0,
        )
        .unwrap();
    assert!(replay.duplicate);
    let copies: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM pool_entries WHERE kind='report'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(copies, 1);
    let peer_commands: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM commands WHERE agent_id=? AND kind='pool'",
            [run2.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(peer_commands, 1);
    // A pool_post reusing the notify's request key lives in its own
    // idempotency scope and cannot collide with or corrupt the copy.
    let other = store
        .pool_write(
            run,
            attempt,
            token,
            message("n1", "a chat message, not the report"),
        )
        .unwrap()
        .unwrap();
    assert!(other.seq > 0);
    let copies: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM pool_entries WHERE kind='report'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(copies, 1);
}

/// Claim order prefers cancel over steer over pool delivery.
#[test]
fn claim_order_prefers_cancel_then_steer_then_pool() {
    let home = common::Home::new();
    let (_, members) = pool(&home, &[("done", "it ships")]);
    let (run, _, _) = &members[0];
    let (peer_run, peer_attempt, peer_token) = &members[1];
    let mut store = home.store();
    // A peer's entry fans one pool command out to this member's tip.
    store
        .pool_write(peer_run, peer_attempt, peer_token, message("c1", "x"))
        .unwrap()
        .unwrap();
    store
        .enqueue(run, "steer", &serde_json::json!({"text":"later"}))
        .unwrap();
    store
        .enqueue(run, "cancel", &serde_json::json!({}))
        .unwrap();
    let (_, first, _) = store.claim_command(run).unwrap().unwrap();
    assert_eq!(first, "cancel");
    let (_, second, _) = store.claim_command(run).unwrap().unwrap();
    assert_eq!(second, "steer");
    let (_, third, _) = store.claim_command(run).unwrap().unwrap();
    assert_eq!(third, "pool");
}

/// A failing fan-out insert rolls back the whole write: no entry, no
/// notification, no delivery outbox row and no command survive, for both a
/// chat write and a notify team copy; a retry after the fault succeeds once.
#[test]
fn forced_fanout_failure_rolls_back_every_row() {
    let home = common::Home::new();
    let (_, members) = pool(&home, &[("done", "it ships")]);
    let (run, attempt, token) = &members[0];
    let mut store = home.store();
    store
        .conn
        .execute(
            "INSERT INTO orchestrator_sessions(id,transport,external_session_id,created_at,last_seen_at) \
             VALUES('os-1','claude_uds','sess-1',1.0,1.0)",
            [],
        )
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE agents SET orchestrator_session_id='os-1' WHERE id=?",
            [run.as_str()],
        )
        .unwrap();
    let count = |store: &agent_run_store::Store, table: &str| -> i64 {
        store
            .conn
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap()
    };
    let before = [
        count(&store, "pool_entries"),
        count(&store, "worker_notifications"),
        count(&store, "deliveries"),
        count(&store, "commands"),
    ];
    store
        .conn
        .execute_batch(
            "CREATE TEMP TRIGGER fail_pool_push BEFORE INSERT ON commands \
             WHEN NEW.kind='pool' BEGIN SELECT RAISE(ABORT,'forced fanout failure'); END;",
        )
        .unwrap();
    assert!(store
        .pool_write(run, attempt, token, message("c1", "hello"))
        .is_err());
    let request = agent_run_domain::worker::NotifyRequest {
        request_id: "n9".into(),
        kind: agent_run_domain::worker::WorkerMessageKind::Notice,
        message: "report".into(),
    };
    assert!(store
        .notify_orchestrator(run, attempt, token, &request, 2.0)
        .is_err());
    let after = [
        count(&store, "pool_entries"),
        count(&store, "worker_notifications"),
        count(&store, "deliveries"),
        count(&store, "commands"),
    ];
    assert_eq!(before, after, "nothing survives a failed fan-out");
    store
        .conn
        .execute_batch("DROP TRIGGER fail_pool_push")
        .unwrap();
    let receipt = store
        .notify_orchestrator(run, attempt, token, &request, 40.0)
        .unwrap();
    assert!(!receipt.duplicate);
    assert_eq!(count(&store, "pool_entries"), before[0] + 1);
}

/// A peer whose tip already ended is not enqueued (nothing would claim the
/// command), while the log entry stays for catch-up.
#[test]
fn terminal_peer_tip_gets_no_pending_command_but_keeps_the_entry() {
    let home = common::Home::new();
    let (_, members) = pool(&home, &[("done", "it ships")]);
    let (run, attempt, token) = &members[0];
    let (run2, _, _) = &members[1];
    let mut store = home.store();
    store
        .conn
        .execute(
            "UPDATE agents SET status='failed' WHERE id=?",
            [run2.as_str()],
        )
        .unwrap();
    store
        .pool_write(run, attempt, token, message("t1", "anyone there"))
        .unwrap()
        .unwrap();
    let pending: i64 = store
        .conn
        .query_row("SELECT COUNT(*) FROM commands WHERE kind='pool'", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(pending, 0);
    let entries: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM pool_entries WHERE kind='message'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(entries, 1);
}

/// The operator's post is stamped by the broker, fans out to every current
/// member once, replays by key, conflicts on a changed body, honours the chat
/// budget and shares the projection members read.
#[test]
fn operator_post_is_stamped_fanned_out_and_idempotent() {
    let home = common::Home::new();
    let (pool_id, members) = pool(&home, &[("done", "it ships")]);
    let pool_id: agent_run_domain::pool::PoolId = pool_id.parse().unwrap();
    let mut store = home.store();
    let first = store
        .pool_operator_post(&pool_id, "op-1", "please prioritise tests")
        .unwrap()
        .unwrap();
    assert!(!first.duplicate && first.seq > 0);
    let (author, direction, kind, agent, sender): (String, String, String, Option<String>, Option<String>) =
        store
            .conn
            .query_row(
                "SELECT author_kind,direction,kind,author_agent_id,sender_run_id FROM pool_entries WHERE seq=?",
                [first.seq as i64],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
            )
            .unwrap();
    assert_eq!(
        (
            author.as_str(),
            direction.as_str(),
            kind.as_str(),
            agent,
            sender
        ),
        ("operator", "team", "message", None, None)
    );
    for (run, _, _) in &members {
        let pending: i64 = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM commands WHERE agent_id=? AND kind='pool'",
                [run.as_str()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(pending, 1, "every current member is pushed once");
    }
    let again = store
        .pool_operator_post(&pool_id, "op-1", "please prioritise tests")
        .unwrap()
        .unwrap();
    assert!(again.duplicate);
    assert_eq!(again.seq, first.seq);
    assert_eq!(
        store
            .pool_operator_post(&pool_id, "op-1", "different body")
            .unwrap(),
        Err(agent_run_domain::pool::PoolDenial::Conflict)
    );
    let commands: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM commands WHERE kind='pool'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(commands, 2, "a replay enqueues nothing");
    let page = store
        .pool_operator_read(&pool_id, 0, None, 50)
        .unwrap()
        .unwrap();
    assert_eq!(page["entries"][0]["author_kind"], "operator");
    assert_eq!(page["status"]["state"], "open");
    assert_eq!(page["status"]["members"].as_array().unwrap().len(), 2);
    assert_eq!(
        page["status"]["replaced_members"].as_array().unwrap().len(),
        0
    );
    let text = page.to_string();
    for private in ["sender_run_id", "attempt_id", "token"] {
        assert!(!text.contains(private), "{private}");
    }
    let unknown: agent_run_domain::pool::PoolId =
        "pool-20260101-000000-0000000000".parse().unwrap();
    assert_eq!(
        store.pool_operator_post(&unknown, "k", "x").unwrap(),
        Err(agent_run_domain::pool::PoolDenial::PoolNotFound)
    );
    assert_eq!(
        store
            .pool_operator_read(&unknown, 0, None, 5)
            .unwrap()
            .unwrap_err(),
        agent_run_domain::pool::PoolDenial::PoolNotFound
    );
}

/// Criteria, member capabilities and pool identity of a voted pool.
type Voted = (
    Vec<(&'static str, &'static str)>,
    Vec<(AgentId, String, String)>,
    agent_run_domain::pool::PoolId,
);

/// A pool whose proposal carries a ready vote from both members, while both
/// executions are still running; returns the criteria, members and store.
fn voted_pool(home: &common::Home) -> Voted {
    let criteria = vec![("done", "it ships")];
    let (pool_id, members) = pool(home, &criteria);
    let mut store = home.store();
    let (run, attempt, token) = &members[0];
    store
        .pool_write(
            run,
            attempt,
            token,
            PoolWrite::Proposal(PoolPropose {
                request_id: "p1".into(),
                message: "result ready".into(),
                snapshot: "commit abc123".into(),
            }),
        )
        .unwrap()
        .unwrap();
    for (n, (run, attempt, token)) in members.iter().enumerate() {
        store
            .pool_write(run, attempt, token, ready(&format!("v{n}"), 1, &criteria))
            .unwrap()
            .unwrap();
    }
    (criteria, members, pool_id.parse().unwrap())
}

/// Ends every member execution `succeeded` and, when `cleaned`, releases
/// ownership with verified cleanup proof.
fn finish_members(store: &Store, members: &[(AgentId, String, String)], cleaned: bool) {
    for (run, _, _) in members {
        store
            .conn
            .execute(
                "UPDATE agents SET status='succeeded' WHERE id=?",
                [run.as_str()],
            )
            .unwrap();
        if cleaned {
            store
                .conn
                .execute(
                    "UPDATE attempts SET ownership_active=0,phase='cleanup_complete',cleanup_proof_json='{}' WHERE agent_id=?",
                    [run.as_str()],
                )
                .unwrap();
        }
    }
}

/// Counts rows of `table`.
fn rows_of(store: &Store, table: &str) -> i64 {
    store
        .conn
        .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
        .unwrap()
}

/// Agreement while members still run, or finished without proven cleanup,
/// never completes; once everything is proven it completes exactly once with
/// one frozen event and one waiting (unbound) outbox row, and a repeat adds nothing.
#[test]
fn completion_needs_agreement_succeeded_tips_and_cleanup() {
    let home = common::Home::new();
    let (_, members, pool_id) = voted_pool(&home);
    let mut store = home.store();
    assert!(
        store.settle_pool(&pool_id).unwrap().is_none(),
        "agreed but running"
    );
    finish_members(&store, &members, false);
    assert!(
        store.settle_pool(&pool_id).unwrap().is_none(),
        "succeeded but cleanup unknown"
    );
    assert_eq!(store.settle_open_pools(20, 0).unwrap(), 0);
    assert_eq!(rows_of(&store, "deliveries"), 0);
    finish_members(&store, &members, true);
    // The maintenance sweep converges once cleanup proof lands later.
    assert_eq!(store.settle_open_pools(20, 7).unwrap(), 1);
    let again = store.settle_pool(&pool_id).unwrap().unwrap();
    assert!(!again.created && !again.bound);
    let (state, delivery, completed): (String, String, f64) = store
        .conn
        .query_row(
            "SELECT state,completion_delivery_id,completed_at FROM pools WHERE id=?",
            [pool_id.as_str()],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(state, "completed");
    assert_eq!(delivery, again.delivery_id);
    assert!(completed > 0.0);
    assert_eq!(rows_of(&store, "deliveries"), 1);
    let events: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM events WHERE kind='pool_completed'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(events, 1);
    let (delivery_state, session): (String, Option<String>) = store
        .conn
        .query_row(
            "SELECT state,orchestrator_session_id FROM deliveries WHERE id=?",
            [&delivery],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        (delivery_state.as_str(), session),
        ("waiting_binding", None)
    );
    let notice: String = store
        .conn
        .query_row(
            "SELECT json_extract(e.data_json,'$.notice') FROM events e JOIN deliveries d ON d.terminal_event_seq=e.seq WHERE d.id=?",
            [&delivery],
            |r| r.get(0),
        )
        .unwrap();
    assert!(notice.contains("commit abc123") && notice.contains("ship it"));
    assert!(notice.contains("member-1 (doer,") && notice.len() <= 4096);
    assert!(!notice.contains("att_"), "no attempt identity: {notice}");
}

/// Every non-success condition leaves the pool open with nothing written.
#[test]
fn failed_missing_stale_or_withdrawn_agreement_never_completes() {
    type Break = fn(&Store, &[(AgentId, String, String)]);
    let cases: [(&str, Break); 8] = [
        ("failed", |s, m| {
            s.conn
                .execute(
                    "UPDATE agents SET status='failed' WHERE id=?",
                    [m[1].0.as_str()],
                )
                .map(|_| ())
                .unwrap()
        }),
        ("timed_out", |s, m| {
            s.conn
                .execute(
                    "UPDATE agents SET status='timed_out' WHERE id=?",
                    [m[1].0.as_str()],
                )
                .map(|_| ())
                .unwrap()
        }),
        ("cancelled", |s, m| {
            s.conn
                .execute(
                    "UPDATE agents SET status='cancelled' WHERE id=?",
                    [m[1].0.as_str()],
                )
                .map(|_| ())
                .unwrap()
        }),
        ("lost", |s, m| {
            s.conn
                .execute(
                    "UPDATE agents SET status='lost' WHERE id=?",
                    [m[1].0.as_str()],
                )
                .map(|_| ())
                .unwrap()
        }),
        ("uncleared ownership", |s, m| {
            s.conn.execute("UPDATE attempts SET ownership_active=1,phase='running',cleanup_proof_json=NULL WHERE agent_id=?", [m[0].0.as_str()]).map(|_| ()).unwrap()
        }),
        ("stale roster", |s, _| {
            s.conn
                .execute("UPDATE pools SET roster_revision=2", [])
                .map(|_| ())
                .unwrap()
        }),
        ("missing vote", |s, m| {
            s.conn
                .execute(
                    "DELETE FROM pool_entries WHERE kind='vote' AND author_agent_id=?",
                    [m[1].0.as_str()],
                )
                .map(|_| ())
                .unwrap()
        }),
        ("unmet check", |s, m| {
            s.conn
                .execute(
                    "INSERT INTO pool_entries(pool_id,author_kind,author_agent_id,author_name,author_role,direction,kind,roster_revision,proposal_seq,decision,checks_json,body,sender_run_id,sender_attempt_id,idem_scope,request_id,created_at) \
                     SELECT e.pool_id,'member',e.author_agent_id,e.author_name,e.author_role,'team','vote',1,1,'ready','[{\"criterion_id\":\"done\",\"status\":\"unmet\",\"evidence\":\"x\"}]','later',e.sender_run_id,e.sender_attempt_id,e.idem_scope,'later-vote',99.0 \
                     FROM pool_entries e WHERE e.kind='vote' AND e.author_agent_id=?",
                    [m[1].0.as_str()],
                )
                .map(|_| ())
                .unwrap()
        }),
    ];
    for (label, break_it) in cases {
        let home = common::Home::new();
        let (_, members, pool_id) = voted_pool(&home);
        let mut store = home.store();
        finish_members(&store, &members, true);
        break_it(&store, &members);
        // `finish_members` made the second member succeeded; the cases then undo that.
        assert!(store.settle_pool(&pool_id).unwrap().is_none(), "{label}");
        assert_eq!(rows_of(&store, "deliveries"), 0, "{label}");
        let state: String = store
            .conn
            .query_row("SELECT state FROM pools", [], |r| r.get(0))
            .unwrap();
        assert_eq!(state, "open", "{label}");
    }
    // Revoke and block votes never complete either.
    for decision in [VoteDecision::Revoke, VoteDecision::Block] {
        let home = common::Home::new();
        let (criteria, members, pool_id) = voted_pool(&home);
        let mut store = home.store();
        let (run, attempt, token) = &members[1];
        store
            .pool_write(
                run,
                attempt,
                token,
                PoolWrite::Vote(PoolVote {
                    request_id: "w".into(),
                    proposal_seq: 1,
                    decision,
                    checks: if decision == VoteDecision::Block {
                        criteria
                            .iter()
                            .map(|(id, _)| CriterionCheck {
                                criterion_id: (*id).into(),
                                status: CheckStatus::Met,
                                evidence: "x".into(),
                            })
                            .collect()
                    } else {
                        vec![]
                    },
                    message: None,
                }),
            )
            .unwrap()
            .unwrap();
        finish_members(&store, &members, true);
        assert!(
            store.settle_pool(&pool_id).unwrap().is_none(),
            "{decision:?}"
        );
    }
}

/// Concurrent settlement records one event and one delivery; a later resume
/// of a member neither changes the frozen status nor mints another notice,
/// and the closed pool refuses new writes.
#[test]
fn concurrent_settlement_is_single_and_the_completed_record_stays_frozen() {
    let home = common::Home::new();
    let (criteria, members, pool_id) = voted_pool(&home);
    finish_members(&home.store(), &members, true);
    let outcomes: Vec<_> = (0..4)
        .map(|_| {
            let (path, pool_id) = (home.path.clone(), pool_id.clone());
            std::thread::spawn(move || {
                Store::open(&path)
                    .unwrap()
                    .settle_pool(&pool_id)
                    .unwrap()
                    .unwrap()
            })
        })
        .collect::<Vec<_>>()
        .into_iter()
        .map(|h| h.join().unwrap())
        .collect();
    assert_eq!(outcomes.iter().filter(|c| c.created).count(), 1);
    assert!(outcomes
        .iter()
        .all(|c| c.delivery_id == outcomes[0].delivery_id));
    let mut store = home.store();
    assert_eq!(rows_of(&store, "deliveries"), 1);
    let frozen = store
        .pool_operator_read(&pool_id, 0, None, 50)
        .unwrap()
        .unwrap();
    assert_eq!(frozen["status"]["state"], "completed");
    assert_eq!(frozen["status"]["agreed"], true);
    let frozen_list = store
        .list_pools(&agent_run_domain::pool::ListPoolsQuery::default())
        .unwrap();
    assert_eq!(frozen_list.items[0].ready, 2);
    assert_eq!(
        frozen_list.items[0].completed_at,
        frozen["status"]["completed_at"].as_f64()
    );
    // A member later resumes independently: a new running tip appears.
    store
        .conn
        .execute(
            "INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,timeout_seconds,config_revision,root_agent_id,parent_agent_id,sequence) \
             SELECT 'ag-20260101-000000-0000000088',runtime,model,profile,'t','t',workdir,'{}','running',9.0,10.0,'cfg',id,id,2 FROM agents WHERE id=?",
            [members[0].0.as_str()],
        )
        .unwrap();
    let after = store
        .pool_operator_read(&pool_id, 0, None, 50)
        .unwrap()
        .unwrap();
    assert_eq!(after["status"], frozen["status"], "history does not drift");
    assert_eq!(
        store
            .list_pools(&agent_run_domain::pool::ListPoolsQuery::default())
            .unwrap(),
        frozen_list
    );
    assert!(store
        .settle_pool(&pool_id)
        .unwrap()
        .is_some_and(|c| !c.created));
    assert_eq!(rows_of(&store, "deliveries"), 1);
    let (run, attempt, token) = &members[1];
    let _ = (run, attempt, token, &criteria);
    assert_eq!(
        store
            .pool_operator_post(&pool_id, "late", "anything")
            .unwrap()
            .unwrap_err(),
        PoolDenial::PoolCompleted
    );
}

/// A pool with an orchestrator binding queues a pending notice; an unbound
/// pool's notice waits, is never expired by the binding window, is not
/// activated by binding one member alone, and `bind_pool` activates it once.
#[test]
fn unbound_completion_waits_for_pool_binding() {
    let reference = agent_run_domain::domain::OrchestratorRef {
        transport: "claude_uds".into(),
        external_session_id: "sess-pool".into(),
        external_turn_id: None,
    };
    // Bound before completion: pending immediately.
    let home = common::Home::new();
    let (_, members, pool_id) = voted_pool(&home);
    let mut store = home.store();
    store
        .conn
        .execute(
            "INSERT INTO orchestrator_sessions(id,transport,external_session_id,created_at,last_seen_at) VALUES('os-p','claude_uds','sess-pool',1.0,1.0)",
            [],
        )
        .unwrap();
    store
        .conn
        .execute("UPDATE pools SET orchestrator_session_id='os-p'", [])
        .unwrap();
    finish_members(&store, &members, true);
    let done = store.settle_pool(&pool_id).unwrap().unwrap();
    assert!(done.bound);
    let state: String = store
        .conn
        .query_row(
            "SELECT state FROM deliveries WHERE id=?",
            [&done.delivery_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(state, "pending");

    // Unbound.
    let home = common::Home::new();
    let (_, members, pool_id) = voted_pool(&home);
    let mut store = home.store();
    finish_members(&store, &members, true);
    let done = store.settle_pool(&pool_id).unwrap().unwrap();
    let state = |store: &Store| -> String {
        store
            .conn
            .query_row(
                "SELECT state FROM deliveries WHERE id=?",
                [&done.delivery_id],
                |r| r.get(0),
            )
            .unwrap()
    };
    assert_eq!(state(&store), "waiting_binding");
    assert!(store.expire_unbound_deliveries(1.0e12).unwrap().is_empty());
    store
        .bind_orchestrator(&members[0].0, &reference, 5.0)
        .unwrap();
    assert_eq!(
        state(&store),
        "waiting_binding",
        "one member's binding is not a pool binding"
    );
    let session = store.bind_pool(&pool_id, &reference, 6.0).unwrap();
    assert_eq!(state(&store), "pending");
    let tips_bound: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM agents WHERE orchestrator_session_id=?",
            [&session],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(tips_bound, 2);
    assert_eq!(
        store.bind_pool(&pool_id, &reference, 7.0).unwrap(),
        session,
        "repeat is a no-op"
    );
    let other = agent_run_domain::domain::OrchestratorRef {
        external_session_id: "someone-else".into(),
        ..reference
    };
    assert!(store.bind_pool(&pool_id, &other, 8.0).is_err());
}

/// The member-terminal hook entry settles the pool its lineage belongs to.
#[test]
fn terminal_member_settles_its_pool() {
    let home = common::Home::new();
    let (_, members, pool_id) = voted_pool(&home);
    let mut store = home.store();
    finish_members(&store, &members, true);
    let done = store.settle_pool_of(&members[1].0).unwrap().unwrap();
    assert!(done.created);
    assert_eq!(done.pool_id, pool_id);
    assert!(
        store.settle_pool_of(&members[1].0).unwrap().is_none(),
        "no open pool remains"
    );
}

/// A closed pool still answers an identical retry of a write that committed
/// earlier with its original receipt, reports a changed body under the same
/// key as a conflict, and refuses any new key.
#[test]
fn closed_pool_replays_identical_retries_and_refuses_new_keys() {
    let home = common::Home::new();
    let (criteria, members, _) = voted_pool(&home);
    let mut store = home.store();
    // Close the pool while the writers are still authenticated, so only the
    // closed-pool guard and the key lookup order are under test.
    store
        .conn
        .execute("UPDATE pools SET state='completed',completed_at=9.0", [])
        .unwrap();
    let (run, attempt, token) = &members[0];
    let proposal = |snapshot: &str| {
        PoolWrite::Proposal(PoolPropose {
            request_id: "p1".into(),
            message: "result ready".into(),
            snapshot: snapshot.into(),
        })
    };
    let replay = store
        .pool_write(run, attempt, token, proposal("commit abc123"))
        .unwrap()
        .unwrap();
    assert!(replay.duplicate && replay.seq == 1);
    assert_eq!(
        store
            .pool_write(run, attempt, token, proposal("a different snapshot"))
            .unwrap()
            .unwrap_err(),
        PoolDenial::Conflict
    );
    assert!(
        store
            .pool_write(run, attempt, token, ready("v0", 1, &criteria))
            .unwrap()
            .unwrap()
            .duplicate,
        "an identical vote retry replays too"
    );
    assert_eq!(
        store
            .pool_write(run, attempt, token, message("fresh-key", "new"))
            .unwrap()
            .unwrap_err(),
        PoolDenial::PoolCompleted
    );
    assert_eq!(rows_of(&store, "pool_entries"), 3, "no row was added");
}

/// The live common-notice projection shares one frozen status across worker and operator reads.
#[test]
fn common_notice_projection_tracks_live_delivery_without_changing_frozen_status() {
    let home = common::Home::new();
    let (pool_id, members) = pool(&home, &[("done", "it ships")]);
    let pool_id: agent_run_domain::pool::PoolId = pool_id.parse().unwrap();
    let (run, attempt, token) = &members[0];
    let mut store = home.store();

    let not_created = store
        .pool_operator_read(&pool_id, 0, None, 50)
        .unwrap()
        .unwrap();
    assert_eq!(not_created["delivery"]["state"], "not_created");
    assert_eq!(not_created["delivery"]["bound"], false);

    let frozen = json!({
        "goal":"ship it",
        "acceptance":[{"id":"done","text":"it ships"}],
        "roster_revision":1,
        "proposal":{"seq":1,"snapshot":"verified result"},
        "members":[
            {"name":"member-1","role":"doer","agent_id":members[0].0,"slot":1},
            {"name":"member-2","role":"doer","agent_id":members[1].0,"slot":2}
        ]
    });
    let tx = store.conn.transaction().unwrap();
    tx.execute(
        "INSERT INTO orchestrator_sessions(id,transport,external_session_id,created_at,last_seen_at) \
         VALUES('pool-session-private','codex_queue','client-session-private',1,1)",
        [],
    ).unwrap();
    tx.execute(
        "INSERT INTO events(agent_id,at,kind,data_json) VALUES(?,2,'pool_completed',?)",
        rusqlite::params![run.as_str(), frozen.to_string()],
    )
    .unwrap();
    let terminal_event: i64 = tx.last_insert_rowid();
    tx.execute(
        "INSERT INTO deliveries(id,agent_id,terminal_event_seq,state) VALUES('ntf_pool_private',?,?,'waiting_binding')",
        rusqlite::params![run.as_str(), terminal_event],
    ).unwrap();
    tx.execute(
        "UPDATE pools SET state='completed',completed_at=2,completion_delivery_id='ntf_pool_private' WHERE id=?",
        [pool_id.as_str()],
    ).unwrap();
    tx.commit().unwrap();
    let proof_before: String = store
        .conn
        .query_row(
            "SELECT data_json FROM events WHERE seq=?",
            [terminal_event],
            |row| row.get(0),
        )
        .unwrap();

    let cases = [
        ("waiting_binding", None, 0, false, None, None),
        (
            "delivered",
            Some("pool-session-private"),
            1,
            false,
            None,
            Some("relay_accepted"),
        ),
        (
            "failed",
            Some("pool-session-private"),
            1,
            true,
            Some("relay_ambiguous"),
            Some("relay_ambiguous"),
        ),
        (
            "retry_wait",
            Some("pool-session-private"),
            2,
            false,
            Some("uds_unavailable"),
            Some("uds_unavailable"),
        ),
        (
            "cancelled",
            Some("pool-session-private"),
            1,
            false,
            Some("relay_rejected"),
            Some("relay_rejected"),
        ),
    ];
    for (state, session, attempts, ambiguous, last_error, classifier) in cases {
        store
            .conn
            .execute(
                "UPDATE pools SET orchestrator_session_id=? WHERE id=?",
                rusqlite::params![session, pool_id.as_str()],
            )
            .unwrap();
        store.conn.execute(
            "UPDATE deliveries SET orchestrator_session_id=?,state=?,attempts=?,ambiguous_result=?,last_error=? WHERE id='ntf_pool_private'",
            rusqlite::params![session, state, attempts, ambiguous, last_error],
        ).unwrap();
        store
            .conn
            .execute(
                "DELETE FROM delivery_attempt_evidence WHERE delivery_id='ntf_pool_private'",
                [],
            )
            .unwrap();
        if let Some(classifier) = classifier {
            let evidence = json!({
                "classifier":classifier,"executable":"desktop-relay","argv_shape":["relay"],
                "duration_ms":1,"returncode":null,"spawn_errno":null,"error_class":null,
                "stdout_tail":"","stderr_tail":"","stdout_bytes":0,"stderr_bytes":0,
                "stdout_truncated":false,"stderr_truncated":false,"message_id_present":state=="delivered"
            });
            store.conn.execute(
                "INSERT INTO delivery_attempt_evidence(delivery_id,attempt,recorded_at,evidence_json) VALUES('ntf_pool_private',?,?,?)",
                rusqlite::params![attempts, 3, evidence.to_string()],
            ).unwrap();
        }

        let operator_page = store
            .pool_operator_read(&pool_id, 0, None, 50)
            .unwrap()
            .unwrap();
        let worker_page = store
            .pool_read(run, attempt, token, 0, None, 50)
            .unwrap()
            .unwrap();
        let delivery = &operator_page["delivery"];
        assert_eq!(delivery, &worker_page["delivery"]);
        assert_eq!(delivery["state"], state);
        assert_eq!(delivery["bound"], session.is_some());
        assert_eq!(delivery["attempts"], attempts);
        assert_eq!(delivery["ambiguous"], ambiguous);
        assert_eq!(
            delivery["last_classification"],
            classifier.map_or(Value::Null, Value::from)
        );
        assert_eq!(
            delivery["last_attempt"]["classifier"],
            classifier.map_or(Value::Null, Value::from)
        );
        assert_eq!(operator_page["status"], worker_page["status"]);
        let printed = delivery.to_string();
        for private in [
            "ntf_pool_private",
            "pool-session-private",
            "client-session-private",
            "orchestrator_session_id",
        ] {
            assert!(!printed.contains(private), "{private} leaked");
        }
    }
    let proof_after: String = store
        .conn
        .query_row(
            "SELECT data_json FROM events WHERE seq=?",
            [terminal_event],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(proof_before, proof_after);
}

/// Reads the operator page's top-level `activity` word.
fn activity(store: &Store, pool_id: &agent_run_domain::pool::PoolId) -> String {
    store
        .pool_operator_read(pool_id, 0, None, 50)
        .unwrap()
        .unwrap()["activity"]
        .as_str()
        .unwrap()
        .to_owned()
}

/// Sets every member's latest execution status and, when `cleaned`, its cleanup proof.
fn set_tips(store: &Store, members: &[(AgentId, String, String)], status: &str, cleaned: bool) {
    for (run, _, _) in members {
        store
            .conn
            .execute(
                "UPDATE agents SET status=? WHERE id=?",
                [status, run.as_str()],
            )
            .unwrap();
        let (phase, proof) = if cleaned {
            ("cleanup_complete", Some("{}"))
        } else {
            ("running", None)
        };
        store
            .conn
            .execute(
                "UPDATE attempts SET ownership_active=?,phase=?,cleanup_proof_json=? WHERE agent_id=?",
                rusqlite::params![!cleaned, phase, proof, run.as_str()],
            )
            .unwrap();
    }
}

/// An open pool's read-time activity follows its current members without ever
/// changing the stored state: cancellation is `stopping` until cleanup is
/// proven, then `cancelled` yet restorable (state stays `open`, nothing is
/// completed or notified), and a resumed member returns it to `running`.
/// Failure, a mixed cancellation or a blocked vote need action; agreed
/// success awaiting its completion record is `settling`; a completed pool
/// stays `completed` with its frozen status byte-identical after a resume.
#[test]
fn pool_activity_projects_lifecycle_without_changing_stored_state() {
    let home = common::Home::new();
    let (_, members, pool_id) = voted_pool(&home);
    let mut store = home.store();
    assert_eq!(activity(&store, &pool_id), "running");

    set_tips(&store, &members, "cancelled", false);
    assert_eq!(activity(&store, &pool_id), "stopping");
    set_tips(&store, &members, "cancelled", true);
    assert_eq!(activity(&store, &pool_id), "cancelled");
    let page = store
        .pool_operator_read(&pool_id, 0, None, 50)
        .unwrap()
        .unwrap();
    assert_eq!(page["status"]["state"], "open");
    assert!(store.settle_pool(&pool_id).unwrap().is_none());
    assert_eq!(rows_of(&store, "deliveries"), 0, "no common completion");

    store
        .conn
        .execute(
            "UPDATE agents SET status='failed' WHERE id=?",
            [members[0].0.as_str()],
        )
        .unwrap();
    assert_eq!(activity(&store, &pool_id), "needs_action", "mixed ends");
    store
        .conn
        .execute(
            "UPDATE agents SET status='cancelled' WHERE id=?",
            [members[0].0.as_str()],
        )
        .unwrap();

    // Resume: a new running tip makes the pool active again.
    store
        .conn
        .execute(
            "INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,timeout_seconds,config_revision,root_agent_id,parent_agent_id,sequence) \
             SELECT 'ag-20260101-000000-0000000077',runtime,model,profile,'t','t',workdir,'{}','running',9.0,10.0,'cfg',id,id,2 FROM agents WHERE id=?",
            [members[0].0.as_str()],
        )
        .unwrap();
    assert_eq!(activity(&store, &pool_id), "running");
    store
        .conn
        .execute(
            "DELETE FROM agents WHERE id='ag-20260101-000000-0000000077'",
            [],
        )
        .unwrap();

    set_tips(&store, &members, "succeeded", false);
    assert_eq!(activity(&store, &pool_id), "stopping");
    set_tips(&store, &members, "succeeded", true);
    assert_eq!(activity(&store, &pool_id), "settling");
    assert_eq!(
        store
            .pool_operator_read(&pool_id, 0, None, 50)
            .unwrap()
            .unwrap()["status"]["state"],
        "open"
    );
    assert!(store.settle_pool(&pool_id).unwrap().is_some());
    assert_eq!(activity(&store, &pool_id), "completed");
    let frozen = store
        .pool_operator_read(&pool_id, 0, None, 50)
        .unwrap()
        .unwrap()["status"]
        .to_string();
    store
        .conn
        .execute(
            "INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,timeout_seconds,config_revision,root_agent_id,parent_agent_id,sequence) \
             SELECT 'ag-20260101-000000-0000000078',runtime,model,profile,'t','t',workdir,'{}','running',9.0,10.0,'cfg',id,id,2 FROM agents WHERE id=?",
            [members[0].0.as_str()],
        )
        .unwrap();
    assert_eq!(activity(&store, &pool_id), "completed");
    let after = store
        .pool_operator_read(&pool_id, 0, None, 50)
        .unwrap()
        .unwrap()["status"]
        .to_string();
    assert_eq!(after, frozen, "frozen status is byte-identical");
}

/// Every member succeeded and cleaned but one vote blocks: the pool cannot
/// settle itself, so the activity asks for action.
#[test]
fn pool_activity_needs_action_for_succeeded_members_with_a_blocked_vote() {
    let home = common::Home::new();
    let (_, members, pool_id) = voted_pool(&home);
    let mut store = home.store();
    let (run, attempt, token) = &members[1];
    store
        .pool_write(
            run,
            attempt,
            token,
            PoolWrite::Vote(PoolVote {
                request_id: "blk".into(),
                proposal_seq: 1,
                decision: VoteDecision::Block,
                checks: vec![],
                message: Some("not acceptable".into()),
            }),
        )
        .unwrap()
        .unwrap();
    set_tips(&store, &members, "succeeded", true);
    assert_eq!(activity(&store, &pool_id), "needs_action");
    assert!(store.settle_pool(&pool_id).unwrap().is_none());
}
