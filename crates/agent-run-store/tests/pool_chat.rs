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
