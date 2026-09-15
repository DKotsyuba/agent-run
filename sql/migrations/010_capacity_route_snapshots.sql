CREATE TABLE IF NOT EXISTS capacity_route_snapshots (
    runtime TEXT NOT NULL,
    scope_id TEXT NOT NULL,
    observed_at REAL NOT NULL,
    valid_until REAL NOT NULL,
    payload_json TEXT NOT NULL,
    PRIMARY KEY (runtime, scope_id),
    CHECK (length(CAST(payload_json AS BLOB)) <= 65536)
);
