-- Worker reports share the delivery outbox while remaining separate from completion projections.
CREATE TABLE worker_capabilities (
  attempt_id TEXT PRIMARY KEY REFERENCES attempts(id) ON DELETE CASCADE,
  token_sha256 TEXT NOT NULL CHECK (length(token_sha256) = 64),
  created_at REAL NOT NULL
);

CREATE TABLE worker_notifications (
  delivery_id TEXT PRIMARY KEY REFERENCES deliveries(id) ON DELETE CASCADE,
  agent_id TEXT NOT NULL REFERENCES agents(id),
  attempt_id TEXT NOT NULL REFERENCES attempts(id),
  request_id TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('notice','risk','question','blocker')),
  message TEXT NOT NULL CHECK (length(CAST(message AS BLOB)) BETWEEN 1 AND 2048),
  created_at REAL NOT NULL,
  UNIQUE (agent_id, request_id)
);
CREATE INDEX idx_worker_notifications_agent_time ON worker_notifications(agent_id, created_at);
