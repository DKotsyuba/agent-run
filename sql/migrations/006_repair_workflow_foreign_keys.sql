-- v6: repair references poisoned by pre-v6 workflow_runs table rebuilds.
-- Rebuilding both referencing tables is safe for clean stores and restores
-- their CREATE text and indexes exactly to the canonical schema.

ALTER TABLE workflow_steps RENAME TO workflow_steps_v5;

CREATE TABLE workflow_steps (
  run_id TEXT NOT NULL REFERENCES workflow_runs(id),
  step_key TEXT NOT NULL,
  spec_json TEXT NOT NULL,
  agent_id TEXT REFERENCES agents(id),
  status TEXT NOT NULL CHECK (
    status IN (
      'pending', 'running', 'succeeded', 'failed', 'skipped', 'cached'
    )
  ),
  result_json TEXT,
  failure_kind TEXT,
  failure_params_json TEXT,
  PRIMARY KEY (run_id, step_key)
);

INSERT INTO workflow_steps (
  run_id, step_key, spec_json, agent_id, status,
  result_json, failure_kind, failure_params_json
)
SELECT
  run_id, step_key, spec_json, agent_id, status,
  result_json, failure_kind, failure_params_json
FROM workflow_steps_v5;

DROP TABLE workflow_steps_v5;

CREATE INDEX idx_workflow_steps_agent
  ON workflow_steps(agent_id)
  WHERE agent_id IS NOT NULL;

ALTER TABLE workflow_deliveries RENAME TO workflow_deliveries_v5;

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
  UNIQUE (run_id)
);

INSERT INTO workflow_deliveries (
  id, run_id, orchestrator_session_id, state, attempts, lease_owner,
  lease_until, next_attempt_at, remote_message_id, last_error, ambiguous_result
)
SELECT
  id, run_id, orchestrator_session_id, state, attempts, lease_owner,
  lease_until, next_attempt_at, remote_message_id, last_error, ambiguous_result
FROM workflow_deliveries_v5;

DROP TABLE workflow_deliveries_v5;

CREATE INDEX workflow_deliveries_due
  ON workflow_deliveries(state, next_attempt_at, lease_until);
