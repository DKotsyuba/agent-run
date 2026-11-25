ALTER TABLE orchestrator_sessions ADD COLUMN owner_uid INTEGER CHECK (owner_uid BETWEEN 0 AND 4294967295);
