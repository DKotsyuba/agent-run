-- Schema v3: persist each workflow run's plan, so a resumed run can relaunch
-- its detached runner with the exact same steps.
--
-- Rebuilds workflow_runs rather than a plain ALTER TABLE ADD COLUMN: SQLite
-- splices an added column into the table's *original* CREATE TABLE text, so
-- the stored schema would drift from how a fresh store writes it.  Keep this
-- delta byte-identical to the matching section of schema.sql, which describes
-- the current schema in full -- test_state_migrations.py compares a migrated
-- store against a freshly created one and fails on any drift.

ALTER TABLE workflow_runs RENAME TO workflow_runs_v2;

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
  plan_json TEXT
);

INSERT INTO workflow_runs
    (id, name, script_sha, status, owner_pid_identity, created_at, finished_at)
  SELECT id, name, script_sha, status, owner_pid_identity, created_at, finished_at
  FROM workflow_runs_v2;

DROP TABLE workflow_runs_v2;

CREATE INDEX idx_workflow_runs_active
  ON workflow_runs(status, created_at, id)
  WHERE status IN ('created', 'running');
