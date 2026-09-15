-- v5: workflow_runs gains result_json (the script's persisted return value).
-- Rebuild instead of ALTER TABLE ADD COLUMN: SQLite splices an added column
-- into the original CREATE TABLE text, which would break the byte-identical
-- migrated-vs-fresh schema equivalence the migration tests enforce (the same
-- reason migration 003 rebuilt this table).
ALTER TABLE workflow_runs RENAME TO workflow_runs_v4;

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
  result_json TEXT,
  orchestrator_session_id TEXT REFERENCES orchestrator_sessions(id)
);

INSERT INTO workflow_runs (
  id, name, script_sha, status, owner_pid_identity,
  created_at, finished_at, plan_json, orchestrator_session_id
)
SELECT
  id, name, script_sha, status, owner_pid_identity,
  created_at, finished_at, plan_json, orchestrator_session_id
FROM workflow_runs_v4;

DROP TABLE workflow_runs_v4;

CREATE INDEX idx_workflow_runs_active
  ON workflow_runs(status, created_at, id)
  WHERE status IN ('created', 'running');
