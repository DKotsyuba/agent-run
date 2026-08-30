-- v7: add the terminal 'expired' state to deliveries.
-- A completion delivery whose agent never carried an orchestrator session
-- reference can never bind, so it is expired once its binding window elapses
-- instead of being rescanned forever.  SQLite bakes a CHECK constraint into the
-- table's CREATE text, so widening the enum means rebuilding the table; the
-- rebuild restores its CREATE text and its index exactly to the canonical
-- schema, which is what tests/test_state_migrations.py compares against.

ALTER TABLE deliveries RENAME TO deliveries_v6;

CREATE TABLE deliveries (
  id TEXT PRIMARY KEY,
  agent_id TEXT NOT NULL REFERENCES agents(id),
  orchestrator_session_id TEXT REFERENCES orchestrator_sessions(id),
  terminal_event_seq INTEGER REFERENCES events(seq),
  state TEXT NOT NULL CHECK (
    state IN (
      'waiting_binding', 'pending', 'sending', 'delivered', 'retry_wait',
      'failed', 'cancelled', 'expired'
    )
  ),
  attempts INTEGER NOT NULL DEFAULT 0,
  lease_owner TEXT,
  lease_until REAL,
  next_attempt_at REAL,
  remote_message_id TEXT,
  last_error TEXT,
  ambiguous_result INTEGER NOT NULL DEFAULT 0
    CHECK (ambiguous_result IN (0, 1)),
  UNIQUE (agent_id, terminal_event_seq)
);

INSERT INTO deliveries (
  id, agent_id, orchestrator_session_id, terminal_event_seq, state, attempts,
  lease_owner, lease_until, next_attempt_at, remote_message_id, last_error,
  ambiguous_result
)
SELECT
  id, agent_id, orchestrator_session_id, terminal_event_seq, state, attempts,
  lease_owner, lease_until, next_attempt_at, remote_message_id, last_error,
  ambiguous_result
FROM deliveries_v6;

DROP TABLE deliveries_v6;

CREATE INDEX idx_deliveries_due
  ON deliveries(state, next_attempt_at, lease_until, id);
