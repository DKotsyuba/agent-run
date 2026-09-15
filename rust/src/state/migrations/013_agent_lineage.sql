-- v13: durable resume lineage.
--
-- Adds the columns a resumed agent needs to record its place in a chain:
-- parent_agent_id (immediate predecessor, NULL for a chain's first agent),
-- root_agent_id (the chain's first agent id; a row is its own root when it
-- has no parent), sequence (1-based position in the chain), and
-- resume_of_runtime_session_id (the source runtime session identity a
-- resuming adapter should continue). The partial UNIQUE index on
-- parent_agent_id is the database-enforced admission guard: at most one
-- child can ever be durably accepted for a given parent, so two concurrent
-- resume attempts of the same agent race for one row and the loser's INSERT
-- fails instead of silently branching the chain.
--
-- Every existing row predates lineage tracking, so it becomes the sole
-- member of its own chain: root_agent_id = id, sequence = 1,
-- parent_agent_id and resume_of_runtime_session_id stay NULL.
--
-- identity_json stays NULL for every existing row too. That NULL is
-- meaningful: it marks a run whose effective account/home/permissions were
-- never snapshotted, so a resume of it cannot prove which identity it ran
-- under and must refuse rather than guess from current configuration.
--
-- root_agent_id carries no REFERENCES: SQLite's ALTER TABLE ADD COLUMN
-- refuses a REFERENCES column with a non-NULL default, and this column
-- needs the '' placeholder default to stay NOT NULL before the backfill
-- below runs. It always holds a real agents.id in practice (itself or an
-- ancestor's), enforced at the application layer alongside parent_agent_id's
-- real foreign key.

ALTER TABLE agents ADD COLUMN parent_agent_id TEXT REFERENCES agents(id);
ALTER TABLE agents ADD COLUMN root_agent_id TEXT NOT NULL DEFAULT '';
ALTER TABLE agents ADD COLUMN sequence INTEGER NOT NULL DEFAULT 1 CHECK (sequence >= 1);
ALTER TABLE agents ADD COLUMN resume_of_runtime_session_id TEXT;
ALTER TABLE agents ADD COLUMN identity_json TEXT;

UPDATE agents SET root_agent_id = id;

CREATE UNIQUE INDEX agents_parent_agent_id_unique
  ON agents(parent_agent_id) WHERE parent_agent_id IS NOT NULL;
