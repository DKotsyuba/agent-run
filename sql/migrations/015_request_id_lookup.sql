CREATE INDEX idx_agents_request_id
  ON agents(request_id) WHERE request_id IS NOT NULL;
