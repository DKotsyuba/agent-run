-- Pool chat is immutable lineage metadata, independent of any execution row.
CREATE TABLE lineage_pool_chat (
  root_agent_id TEXT PRIMARY KEY NOT NULL,
  chat_json TEXT NOT NULL
);
CREATE TRIGGER lineage_pool_chat_immutable BEFORE UPDATE ON lineage_pool_chat
BEGIN
  SELECT RAISE(ABORT, 'pool chat seat is immutable');
END;
CREATE TRIGGER lineage_pool_chat_prune AFTER DELETE ON agents
WHEN EXISTS (
  SELECT 1 FROM lineage_pool_chat WHERE root_agent_id=COALESCE(NULLIF(OLD.root_agent_id,''),OLD.id)
) AND NOT EXISTS (
  SELECT 1 FROM agents WHERE id=COALESCE(NULLIF(OLD.root_agent_id,''),OLD.id)
     OR root_agent_id=COALESCE(NULLIF(OLD.root_agent_id,''),OLD.id)
)
BEGIN
  DELETE FROM lineage_pool_chat WHERE root_agent_id=COALESCE(NULLIF(OLD.root_agent_id,''),OLD.id);
END;
