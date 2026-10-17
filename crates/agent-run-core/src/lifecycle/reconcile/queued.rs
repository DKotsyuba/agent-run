//! Online recovery: prepare process/file evidence outside SQL, then CAS through writer.

use super::*;
use crate::broker_writer::{BROKER_BUDGET, BrokerLane};
use agent_run_store::ops;
use std::path::Path;

/// Restore up to limit executions using the same evidence predicates as legacy recovery.
/// No writable connection opens here and no process/filesystem action runs in SQL.
pub async fn reconcile(home: &Path, lane: &BrokerLane, limit: usize) -> Result<Vec<AgentId>> {
    if !(1..=1000).contains(&limit) {
        return Err(invalid("reconciliation limit must be 1..1000"));
    }
    let mut changed = Vec::new();
    for (name, select) in [
        ("unowned_starting", unowned_sql()),
        ("active_supervisors", active_sql()),
    ] {
        let remaining = limit.saturating_sub(changed.len());
        if remaining == 0 {
            break;
        }
        let root = home.to_owned();
        let rows = tokio::task::spawn_blocking(move || {
            fair_read(&Store::open(&root)?, name, select, remaining)
        })
        .await
        .map_err(|_| Error::Runtime("reconciliation reader stopped".into()))??;
        if let Some(last) = rows.last() {
            lane.local(
                ops::ReconcileReapedAgent {
                    cursor: name.into(),
                    created_at: last.created_at,
                    execution: last.id.clone(),
                    attempt: None,
                },
                BROKER_BUDGET,
            )
            .await?;
        }
        for row in rows {
            let (state, kind) = if name == "unowned_starting" {
                let Some(owner) = startup_owner(&row) else {
                    continue;
                };
                (
                    process::observe(Some(owner.pid), Some(&owner.token), owner.birth),
                    "unowned_starting",
                )
            } else {
                let (Some(pid), Some(identity)) =
                    (row.supervisor_pid, row.supervisor_identity.as_deref())
                else {
                    continue;
                };
                if row.process_group_id.is_none() && row.status != Status::Starting {
                    continue;
                }
                let state = process::observe(Some(pid), Some(identity), row.supervisor_birth_time);
                (
                    state,
                    if state == ProcessState::Reused {
                        "supervisor_identity_mismatch"
                    } else {
                        "supervisor_dead"
                    },
                )
            };
            if !proved_gone(state) {
                continue;
            }
            let root = home.to_owned();
            let expected = row.clone();
            let finish = tokio::task::spawn_blocking(move || prepare_finish(&root, &expected))
                .await
                .map_err(|_| Error::Runtime("finish recovery preparation stopped".into()))??;
            let applied = match finish {
                FinishPreparation::Pending => false,
                FinishPreparation::Ready(op, captured) => {
                    if let Some((owner, snapshot)) = captured {
                        persist_process_batches(lane, owner, snapshot).await?;
                    }
                    lane.local(*op, BROKER_BUDGET).await?
                }
                FinishPreparation::Absent => lane.local(ops::MarkLost {
                    expected: ownership(&row), failure_kind: kind.into(),
                    failure_text: "periodic detached supervisor reconciliation".into(),
                    evidence: json!({"verdict":state_name(state),"supervisor_pid":row.supervisor_pid,"process_group_id":row.process_group_id}),
                    checked_at: now(),
                }, BROKER_BUDGET).await?,
            };
            if applied {
                let reader = Store::open(home)?;
                let _ = crate::housekeeping::compact_process_artifacts(
                    home,
                    &reader.conn,
                    row.id.as_str(),
                    64,
                );
                changed.push(row.id);
            }
        }
    }
    let root = home.to_owned();
    let prepared = tokio::task::spawn_blocking(move || prepare_orphans(&root, limit))
        .await
        .map_err(|_| Error::Runtime("orphan recovery preparation stopped".into()))??;
    if let Some(cursor) = prepared.cursor {
        lane.local(cursor, BROKER_BUDGET).await?;
    }
    for captured in prepared.captured {
        persist_process_batches(lane, captured.owner, captured.snapshot).await?;
    }
    for release in prepared.releases {
        let id = release.execution.clone();
        lane.local(release, BROKER_BUDGET).await?;
        let reader = Store::open(home)?;
        let _ = crate::housekeeping::compact_process_artifacts(home, &reader.conn, id.as_str(), 64);
    }
    Ok(changed)
}

/// Copy exact ownership facts for writer-side CAS; status/heartbeat are rechecked there.
fn ownership(row: &Candidate) -> ops::ExpectedOwnership {
    ops::ExpectedOwnership {
        execution: row.id.clone(),
        supervisor_pid: row.supervisor_pid,
        supervisor_identity: row.supervisor_identity.clone(),
        supervisor_birth_time: row.supervisor_birth_time,
        process_group_id: row.process_group_id,
        startup_owner: row.startup_owner.clone(),
        startup_birth_time: row.startup_birth_time,
    }
}

/// Read one fair keyset page without advancing its cursor or acquiring a write lock.
fn fair_read(store: &Store, name: &str, select: &str, limit: usize) -> Result<Vec<Candidate>> {
    let cursor: Option<(f64, String)> = store
        .conn
        .query_row(
            "SELECT created_at,agent_id FROM reconciliation_cursors WHERE name=?",
            [name],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let mut rows = match &cursor {
        Some((at, id)) => read_candidates(
            store,
            &format!(
                "{select} AND (created_at>?1 OR (created_at=?1 AND id>?2)) ORDER BY created_at,id LIMIT ?3"
            ),
            params![at, id, limit as i64],
        )?,
        None => read_candidates(
            store,
            &format!("{select} ORDER BY created_at,id LIMIT ?1"),
            [limit as i64],
        )?,
    };
    if let Some((at, id)) = cursor
        && rows.len() < limit
    {
        let remaining = (limit - rows.len()) as i64;
        rows.extend(read_candidates(store, &format!("{select} AND (created_at<?1 OR (created_at=?1 AND id<=?2)) ORDER BY created_at,id LIMIT ?3"), params![at,id,remaining])?);
    }
    Ok(rows)
}

/// Whether explicit finish prevents ordinary loss or supplies a verified terminal unit.
enum FinishPreparation {
    /// No accepted, observed finish exists; normal loss predicates apply.
    Absent,
    /// Finish exists but mandatory cleanup/identity/answer evidence is incomplete.
    Pending,
    /// OS cleanup and sealed immutable answer prepared outside SQL.
    Ready(
        Box<ops::RecoverFinish>,
        Option<(String, process::OwnershipSnapshot)>,
    ),
}

/// Prepare an accepted explicit finish without mutating SQL or replacing history seals.
fn prepare_finish(home: &Path, expected: &Candidate) -> Result<FinishPreparation> {
    let store = Store::open(home)?;
    let row = store.get(&expected.id)?;
    if !row.request.explicit_finish || row.status.terminal() {
        return Ok(FinishPreparation::Absent);
    }
    let Some(intent) = store.worker_finish_intent(&row.id, true)? else {
        return Ok(FinishPreparation::Absent);
    };
    let current = read_candidates(&store, "SELECT * FROM agents WHERE id=?", [row.id.as_str()])?;
    if current
        .first()
        .is_none_or(|current| !same_ownership(expected, current))
        || !proved_gone(process::observe(
            expected.supervisor_pid,
            expected.supervisor_identity.as_deref(),
            expected.supervisor_birth_time,
        ))
    {
        return Ok(FinishPreparation::Pending);
    }
    let attempt: Option<FinishRecoveryAttempt> = store.conn.query_row(
        "SELECT id,process_identity,process_birth_time,cleanup_proof_json FROM attempts WHERE agent_id=? AND ownership_active=1 AND finished_at IS NULL",
        [row.id.as_str()], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)),
    ).optional()?;
    let Some((attempt, token, birth, saved)) = attempt else {
        return Ok(FinishPreparation::Pending);
    };
    if !cleanup_mcp_discovery(&store, &attempt)? {
        return Ok(FinishPreparation::Pending);
    }
    let mut captured = None;
    let cleanup = match saved
        .as_deref()
        .map(serde_json::from_str::<process::Cleanup>)
        .transpose()?
    {
        Some(proof) if proof.confirmed && proof.group_gone => proof,
        _ => {
            let Some(mut owned) = store.remembered_processes("attempt", &attempt)? else {
                return Ok(FinishPreparation::Pending);
            };
            if !owned.leader.as_ref().is_some_and(|leader| {
                Some(leader.token.as_str()) == token.as_deref() && Some(leader.birth) == birth
            }) {
                return Ok(FinishPreparation::Pending);
            }
            let proof = owned.cleanup_blocking(ORPHAN_GRACE)?;
            captured = owned.snapshot().map(|snapshot| (attempt.clone(), snapshot));
            if !proof.confirmed || !proof.group_gone {
                return Ok(FinishPreparation::Pending);
            }
            proof
        }
    };
    let root = store.home.join("agents").join(row.id.as_str());
    agent_run_platform::fs::private_dir(&root)?;
    let answer = agent_run_platform::verify::seal(&root, Path::new("answer.md"), &intent.summary)?;
    Ok(FinishPreparation::Ready(
        Box::new(ops::RecoverFinish {
            expected: ownership(expected),
            cleanup: serde_json::to_value(cleanup)?,
            proof: Some(answer),
            checked_at: now(),
        }),
        captured,
    ))
}

/// Bounded orphan actions whose effects will be committed by the writer afterwards.
struct OrphanPreparation {
    /// Exact execution/attempt-bound cursor advance.
    cursor: Option<ops::ReconcileReapedAgent>,
    /// Captured process updates made while cleaning, without SQL writes.
    captured: Vec<ops::RememberProcesses>,
    /// Proof-gated release or bounded unresolved observation per examined attempt.
    releases: Vec<ops::ReleaseOrphanedAttempt>,
}

/// Prepare orphan cleanup using only recorded immutable identity and native absence proof.
fn prepare_orphans(home: &Path, limit: usize) -> Result<OrphanPreparation> {
    let store = Store::open(home)?;
    let select = "SELECT t.id,t.agent_id,t.phase,t.process_identity,t.process_birth_time,t.cleanup_proof_json,a.process_group_id,t.created_at FROM attempts t JOIN agents a ON a.id=t.agent_id WHERE a.status IN ('succeeded','failed','cancelled','lost','timed_out') AND (t.ownership_active=1 OR (t.phase='cleanup_complete' AND json_extract(t.cleanup_proof_json,'$.confirmed')=1 AND EXISTS(SELECT 1 FROM process_ownership p WHERE p.owner_kind='attempt' AND p.owner_id=t.id)))";
    let cursor: Option<(f64, String)> = store
        .conn
        .query_row(
            "SELECT created_at,agent_id FROM reconciliation_cursors WHERE name='orphaned_attempts'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    let page = |tail: &str, args: &[&dyn rusqlite::ToSql]| -> Result<Vec<(OwnedAttempt, f64)>> {
        Ok(store
            .conn
            .prepare(&format!("{select}{tail}"))?
            .query_map(args, |r| {
                Ok((
                    OwnedAttempt {
                        id: r.get(0)?,
                        agent: r.get(1)?,
                        phase: r.get(2)?,
                        token: r.get(3)?,
                        birth: r.get(4)?,
                        proof: r.get(5)?,
                        group: r.get(6)?,
                    },
                    r.get(7)?,
                ))
            })?
            .collect::<rusqlite::Result<_>>()?)
    };
    let cap = limit as i64;
    let mut rows = match &cursor {
        Some((at, id)) => page(
            " AND (t.created_at>?1 OR (t.created_at=?1 AND t.id>?2)) ORDER BY t.created_at,t.id LIMIT ?3",
            &[at, id, &cap],
        )?,
        None => page(" ORDER BY t.created_at,t.id LIMIT ?1", &[&cap])?,
    };
    if let Some((at, id)) = &cursor
        && rows.len() < limit
    {
        let remaining = (limit - rows.len()) as i64;
        rows.extend(page(" AND (t.created_at<?1 OR (t.created_at=?1 AND t.id<=?2)) ORDER BY t.created_at,t.id LIMIT ?3", &[at,id,&remaining])?);
    }
    let mut prepared = OrphanPreparation {
        cursor: rows
            .last()
            .map(|(last, at)| {
                Ok::<_, Error>(ops::ReconcileReapedAgent {
                    cursor: "orphaned_attempts".into(),
                    created_at: *at,
                    execution: last.agent.parse()?,
                    attempt: Some(last.id.clone()),
                })
            })
            .transpose()?,
        captured: Vec::new(),
        releases: Vec::new(),
    };
    for (attempt, _) in rows {
        if !cleanup_mcp_discovery(&store, &attempt.id)? {
            continue;
        }
        let saved = store.remembered_processes("attempt", &attempt.id)?;
        let confirmed = attempt
            .proof
            .as_deref()
            .and_then(|v| serde_json::from_str::<serde_json::Value>(v).ok())
            .is_some_and(|v| v["confirmed"] == true || v["never_spawned"] == true);
        let outcome = match (&attempt.token, attempt.birth, attempt.group) {
            _ if confirmed && attempt.phase.as_deref() == Some("cleanup_complete") => {
                ops::OrphanOutcome::Release { proof: None }
            }
            (None, _, _) if attempt.phase.as_deref() == Some("prepared") => {
                ops::OrphanOutcome::Release {
                    proof: Some(json!({"never_spawned":true,"reconciled":true})),
                }
            }
            (Some(token), Some(birth), group) => {
                let mut owned = match saved {
                    Some(owned)
                        if owned.leader.as_ref().is_some_and(|r| {
                            r.pid > 1 && r.group == r.pid && r.token == *token && r.birth == birth
                        }) =>
                    {
                        owned
                    }
                    Some(_) => {
                        prepared.releases.push(ops::ReleaseOrphanedAttempt {
                            execution: attempt.agent.parse()?,
                            attempt: attempt.id.clone(),
                            outcome: ops::OrphanOutcome::Unresolved {
                                reason: "stored_process_identity_mismatch".into(),
                            },
                        });
                        continue;
                    }
                    None => match group {
                        Some(pid) if pid > 1 => {
                            match process::observe(Some(pid), Some(token), Some(birth)) {
                                ProcessState::Alive => {
                                    process::OwnedProcess::adopt(process::Identity {
                                        pid,
                                        ppid: 0,
                                        group: pid,
                                        birth,
                                        token: token.clone(),
                                        program: None,
                                        zombie: false,
                                    })
                                }
                                ProcessState::Dead | ProcessState::Reused => {
                                    prepared.releases.push(ops::ReleaseOrphanedAttempt {
                                        execution: attempt.agent.parse()?,
                                        attempt: attempt.id.clone(),
                                        outcome: ops::OrphanOutcome::Unresolved {
                                            reason: "leader_gone_descendants_unverifiable".into(),
                                        },
                                    });
                                    continue;
                                }
                                _ => continue,
                            }
                        }
                        _ => {
                            prepared.releases.push(ops::ReleaseOrphanedAttempt {
                                execution: attempt.agent.parse()?,
                                attempt: attempt.id.clone(),
                                outcome: ops::OrphanOutcome::Unresolved {
                                    reason: "no_process_evidence".into(),
                                },
                            });
                            continue;
                        }
                    },
                };
                let cleanup = owned.cleanup_blocking(ORPHAN_GRACE);
                if let Some(snapshot) = owned.snapshot() {
                    prepared.captured.push(ops::RememberProcesses {
                        kind: "attempt".into(),
                        owner: attempt.id.clone(),
                        snapshot,
                    });
                }
                match cleanup {
                    Ok(proof) if proof.confirmed => {
                        let mut proof = serde_json::to_value(proof)?;
                        proof["reconciled"] = json!(true);
                        ops::OrphanOutcome::Release { proof: Some(proof) }
                    }
                    Ok(_) => ops::OrphanOutcome::Unresolved {
                        reason: "cleanup_unconfirmed".into(),
                    },
                    Err(_) => continue,
                }
            }
            _ => ops::OrphanOutcome::Unresolved {
                reason: "no_process_evidence".into(),
            },
        };
        prepared.releases.push(ops::ReleaseOrphanedAttempt {
            execution: attempt.agent.parse()?,
            attempt: attempt.id,
            outcome,
        });
    }
    Ok(prepared)
}

/// Sends restored recovery evidence in independently validated batches, never a size-limited full history.
/// Idempotent store insertion tolerates previously committed members; writer admission stays bounded.
async fn persist_process_batches(
    lane: &BrokerLane,
    owner: String,
    snapshot: process::OwnershipSnapshot,
) -> Result<()> {
    let captured = process::OwnedProcess::restore(snapshot)?;
    let mut offset = 0;
    while offset < captured.capture_revision().0 {
        let (snapshot, end) = captured
            .snapshot_since(offset)
            .ok_or_else(|| invalid("recovery process root is unavailable"))?;
        lane.remote(
            ops::RememberProcesses {
                kind: "attempt".into(),
                owner: owner.clone(),
                snapshot,
            },
            BROKER_BUDGET,
        )
        .await?;
        offset = end;
    }
    Ok(())
}
