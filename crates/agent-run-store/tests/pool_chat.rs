//! Authenticated pool chat, proposals, votes, budgets and derived status.

mod common;

use agent_run_domain::domain::AgentId;
use agent_run_domain::pool::{
    CheckStatus, CriterionCheck, PoolDenial, PoolMessage, PoolPropose, PoolVote, VoteDecision,
};
use agent_run_store::pool_log::PoolWrite;
use agent_run_store::Store;
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
                rusqlite::params![id.as_str(), slot, format!("member-{slot}")],
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

/// Reads page by the immutable cursor in both directions and report no
/// internal execution identity.
#[test]
fn read_pages_by_cursor_without_internal_ids() {
    let home = common::Home::new();
    let (_, members) = pool(&home, &[("done", "it ships")]);
    let (run, attempt, token) = &members[0];
    let mut store = home.store();
    for index in 1..=3 {
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
    assert_eq!(rest["entries"].as_array().unwrap().len(), 1);
    let back = store
        .pool_read(run, attempt, token, 0, Some(3), 50)
        .unwrap()
        .unwrap();
    let bodies: Vec<&str> = back["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| entry["body"].as_str().unwrap())
        .collect();
    assert_eq!(bodies, ["m1", "m2"]);
    let text = rest.to_string();
    assert!(!text.contains(&attempt.clone()), "no attempt ids leak");
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
