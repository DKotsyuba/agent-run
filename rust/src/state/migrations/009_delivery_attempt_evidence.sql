CREATE TABLE delivery_attempt_evidence (
    delivery_id TEXT NOT NULL REFERENCES deliveries(id) ON DELETE CASCADE,
    attempt INTEGER NOT NULL CHECK (attempt > 0),
    recorded_at REAL NOT NULL,
    evidence_json TEXT NOT NULL CHECK (length(CAST(evidence_json AS BLOB)) <= 16384),
    PRIMARY KEY (delivery_id, attempt)
);
