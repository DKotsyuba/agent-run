CREATE TABLE managed_service_generations (
    id TEXT PRIMARY KEY,
    service_id TEXT NOT NULL,
    revision TEXT NOT NULL CHECK(length(revision) = 64),
    definition_json TEXT NOT NULL CHECK(json_valid(definition_json) AND length(CAST(definition_json AS BLOB)) <= 1048576),
    state TEXT NOT NULL CHECK(state IN ('starting','ready','unhealthy','stopping','stopped','unknown')),
    broker_identity_json TEXT NOT NULL CHECK(json_valid(broker_identity_json)),
    process_identity_json TEXT CHECK(process_identity_json IS NULL OR json_valid(process_identity_json)),
    created_at REAL NOT NULL,
    ready_at REAL,
    checked_at REAL,
    idle_since REAL,
    failure_kind TEXT CHECK(failure_kind IS NULL OR length(failure_kind) <= 64),
    cleanup_json TEXT CHECK(cleanup_json IS NULL OR json_valid(cleanup_json))
);
CREATE UNIQUE INDEX idx_managed_service_live
    ON managed_service_generations(service_id) WHERE state != 'stopped';

CREATE TABLE agent_service_gates (
    agent_id TEXT PRIMARY KEY REFERENCES agents(id),
    state TEXT NOT NULL CHECK(state IN ('pending','ready','failed')),
    failure_kind TEXT CHECK(failure_kind IS NULL OR length(failure_kind) <= 64)
);
CREATE INDEX idx_agent_service_gates_pending ON agent_service_gates(agent_id) WHERE state='pending';
CREATE TABLE managed_service_leases (
    generation_id TEXT NOT NULL REFERENCES managed_service_generations(id),
    agent_id TEXT NOT NULL REFERENCES agents(id),
    acquired_at REAL NOT NULL,
    released_at REAL,
    PRIMARY KEY(generation_id, agent_id)
);
CREATE INDEX idx_managed_service_lease_active
    ON managed_service_leases(generation_id, agent_id) WHERE released_at IS NULL;

CREATE TABLE managed_service_probes (
    id TEXT PRIMARY KEY,
    generation_id TEXT NOT NULL REFERENCES managed_service_generations(id),
    created_at REAL NOT NULL
);

CREATE TABLE process_ownership (
    owner_kind TEXT NOT NULL CHECK(owner_kind IN ('attempt','service','probe')),
    owner_id TEXT NOT NULL,
    leader_json TEXT NOT NULL CHECK(json_valid(leader_json)),
    descendants_observed INTEGER NOT NULL CHECK(descendants_observed IN (0,1)),
    updated_at REAL NOT NULL,
    PRIMARY KEY(owner_kind, owner_id)
);
CREATE TABLE process_members (
    owner_kind TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    pid INTEGER NOT NULL CHECK(pid > 1),
    token TEXT NOT NULL CHECK(length(token) > 0),
    identity_json TEXT NOT NULL CHECK(json_valid(identity_json)),
    PRIMARY KEY(pid, token),
    FOREIGN KEY(owner_kind, owner_id) REFERENCES process_ownership(owner_kind, owner_id)
);
CREATE INDEX idx_process_members_owner ON process_members(owner_kind, owner_id);
