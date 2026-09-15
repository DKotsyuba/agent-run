CREATE TABLE reconciliation_cursors (
  name TEXT PRIMARY KEY,
  created_at REAL NOT NULL,
  agent_id TEXT NOT NULL
);
