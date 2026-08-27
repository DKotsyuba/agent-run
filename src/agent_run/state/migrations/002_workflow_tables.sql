-- Schema v2: workflow runs and their steps.
--
-- Keep this delta byte-identical to the matching section of schema.sql, which
-- describes the current schema in full.  test_state_migrations.py compares a
-- migrated store against a freshly created one and fails on any drift.

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
  finished_at REAL
);

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

CREATE INDEX idx_workflow_runs_active
  ON workflow_runs(status, created_at, id)
  WHERE status IN ('created', 'running');
CREATE INDEX idx_workflow_steps_agent
  ON workflow_steps(agent_id)
  WHERE agent_id IS NOT NULL;
