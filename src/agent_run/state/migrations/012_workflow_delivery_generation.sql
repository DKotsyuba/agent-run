-- v12: one immutable workflow notice per terminal transition, not per run.
--
-- UNIQUE (run_id) let a run produce exactly one lifecycle notice for its whole
-- life, so an ordinary failed -> resume -> succeeded run could not record its
-- second terminal transition: the INSERT in finish_workflow_run hit the
-- constraint and rolled the terminal transaction back, leaving the run
-- 'running' forever.  Each terminal transition now claims its own
-- attempt_generation.
--
-- The dispatcher used to read the announced status by joining the *mutable*
-- workflow_runs row, so a notice queued for a failed attempt and delivered
-- after a resume announced the later result.  run_status and result_json
-- snapshot the transition the notice was created for, and are never updated
-- afterwards.
--
-- Existing rows: a notice whose run is still terminal is the notice for that
-- terminal status, so it is backfilled from the run and stays deliverable at
-- generation 1.  A notice whose run has since been resumed back to
-- 'created'/'running' was queued for a terminal status this schema can no
-- longer recover; it is retired as 'cancelled' rather than left to announce a
-- status it never observed.  A 'sending' row is retired the same way: its
-- lease still settles it by id, and its snapshot is only read at claim time.

ALTER TABLE workflow_deliveries RENAME TO workflow_deliveries_v11;

CREATE TABLE workflow_deliveries (
  id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES workflow_runs(id),
  orchestrator_session_id TEXT NOT NULL REFERENCES orchestrator_sessions(id),
  state TEXT NOT NULL CHECK (
    state IN ('pending', 'sending', 'delivered', 'retry_wait', 'failed', 'cancelled')
  ),
  attempts INTEGER NOT NULL DEFAULT 0,
  lease_owner TEXT,
  lease_until REAL,
  next_attempt_at REAL,
  remote_message_id TEXT,
  last_error TEXT,
  ambiguous_result INTEGER NOT NULL DEFAULT 0 CHECK (ambiguous_result IN (0, 1)),
  attempt_generation INTEGER NOT NULL DEFAULT 1 CHECK (attempt_generation >= 1),
  run_status TEXT NOT NULL CHECK (
    run_status IN ('succeeded', 'failed', 'cancelled', 'lost')
  ),
  result_json TEXT,
  UNIQUE (run_id, attempt_generation)
);

INSERT INTO workflow_deliveries (
  id, run_id, orchestrator_session_id, state, attempts, lease_owner,
  lease_until, next_attempt_at, remote_message_id, last_error, ambiguous_result,
  attempt_generation, run_status, result_json
)
SELECT
  old.id, old.run_id, old.orchestrator_session_id,
  CASE WHEN run.status IN ('succeeded', 'failed', 'cancelled', 'lost')
       THEN old.state ELSE 'cancelled' END,
  old.attempts,
  CASE WHEN run.status IN ('succeeded', 'failed', 'cancelled', 'lost')
       THEN old.lease_owner ELSE NULL END,
  CASE WHEN run.status IN ('succeeded', 'failed', 'cancelled', 'lost')
       THEN old.lease_until ELSE NULL END,
  CASE WHEN run.status IN ('succeeded', 'failed', 'cancelled', 'lost')
       THEN old.next_attempt_at ELSE NULL END,
  old.remote_message_id,
  CASE WHEN run.status IN ('succeeded', 'failed', 'cancelled', 'lost')
       THEN old.last_error
       ELSE 'retired by schema v12: attempt status was not recorded' END,
  old.ambiguous_result,
  1,
  CASE WHEN run.status IN ('succeeded', 'failed', 'cancelled', 'lost')
       THEN run.status ELSE 'lost' END,
  CASE WHEN run.status IN ('succeeded', 'failed', 'cancelled', 'lost')
       THEN run.result_json ELSE NULL END
FROM workflow_deliveries_v11 old
JOIN workflow_runs run ON run.id = old.run_id;

DROP TABLE workflow_deliveries_v11;

CREATE INDEX workflow_deliveries_due
  ON workflow_deliveries(state, next_attempt_at, lease_until);
