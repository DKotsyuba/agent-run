ALTER TABLE workflow_steps RENAME TO workflow_steps_v3;
ALTER TABLE workflow_runs RENAME TO workflow_runs_v3;

CREATE TABLE workflow_runs (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  script_sha TEXT NOT NULL,
  status TEXT NOT NULL CHECK (
    status IN (
      'created', 'running', 'succeeded', 'failed', 'cancelled', 'lost'
    )
  ),
  owner_pid_identity TEXT,
  created_at REAL NOT NULL,
  finished_at REAL,
  plan_json TEXT,
  orchestrator_session_id TEXT REFERENCES orchestrator_sessions(id)
);

INSERT INTO workflow_runs
  (id, name, script_sha, status, owner_pid_identity, created_at, finished_at, plan_json)
SELECT id, name, script_sha, status, owner_pid_identity, created_at, finished_at, plan_json
FROM workflow_runs_v3;

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

INSERT INTO workflow_steps
SELECT run_id, step_key, spec_json, agent_id, status, result_json,
       failure_kind, failure_params_json
FROM workflow_steps_v3;

DROP TABLE workflow_steps_v3;
DROP TABLE workflow_runs_v3;

CREATE INDEX idx_workflow_runs_active
  ON workflow_runs(status, created_at, id)
  WHERE status IN ('created', 'running');

CREATE INDEX idx_workflow_steps_agent
  ON workflow_steps(agent_id)
  WHERE agent_id IS NOT NULL;

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

CREATE INDEX workflow_deliveries_due
  ON workflow_deliveries(state, next_attempt_at, lease_until);

PRAGMA user_version = 4;
