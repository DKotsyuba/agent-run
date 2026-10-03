-- Native error evidence has no historical backfill: old measurements are unknown.
ALTER TABLE messages ADD COLUMN error INTEGER CHECK (error IN (0,1));
ALTER TABLE messages ADD COLUMN error_source TEXT;
ALTER TABLE messages ADD COLUMN content_complete INTEGER CHECK (content_complete IN (0,1));
-- Stable lineage indexing bounds reverse scans without loading retained history.
ALTER TABLE messages ADD COLUMN root_agent_id TEXT NOT NULL DEFAULT '';
UPDATE messages SET root_agent_id=(SELECT COALESCE(NULLIF(a.root_agent_id,''),a.id) FROM agents a WHERE a.id=messages.agent_id);
CREATE INDEX idx_messages_root_seq ON messages(root_agent_id,seq);
