-- One registry row per canonical runtime home records the versioned physical
-- storage layout its shared runtime assets were laid out under. A `prepared`
-- row pins every shared reference the layout installed and blocks admission
-- into that home until an explicit commit or owner-proven recovery replaces
-- it; it never expires by age. A `committed` row only describes the mapping
-- while the home exists. No blob reference counts live here: pinning is the
-- row state itself, never a counter a partial failure could leave wrong.
CREATE TABLE runtime_storage_layouts (
    runtime_home TEXT PRIMARY KEY
        CHECK (length(runtime_home) > 1 AND runtime_home LIKE '/%'),
    index_sha256 TEXT NOT NULL CHECK (length(index_sha256) = 64),
    layout_json TEXT NOT NULL
        CHECK (json_valid(layout_json) AND length(CAST(layout_json AS BLOB)) <= 65536),
    layout_sha256 TEXT NOT NULL CHECK (length(layout_sha256) = 64),
    state TEXT NOT NULL CHECK (state IN ('prepared','committed')),
    operation_token TEXT NOT NULL CHECK (length(operation_token) BETWEEN 16 AND 64),
    owner_agent_id TEXT,
    updated_at REAL NOT NULL
);
CREATE INDEX idx_runtime_storage_layouts_pending
    ON runtime_storage_layouts(updated_at) WHERE state='prepared';
