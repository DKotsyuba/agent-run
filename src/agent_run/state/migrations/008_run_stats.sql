-- v8: the normalized per-run statistics table.
-- Token and timing data used to live only inside runtime-specific event
-- payloads; run_stats carries one queryable row per agent.  Columns stay NULL
-- when the runtime never reported them -- a missing measurement is never
-- backfilled with a fabricated zero.

CREATE TABLE run_stats (
  agent_id TEXT PRIMARY KEY REFERENCES agents(id),
  runtime TEXT NOT NULL,
  model TEXT NOT NULL,
  profile TEXT NOT NULL,
  status TEXT NOT NULL,
  failure_kind TEXT,
  started_at REAL,
  finished_at REAL,
  duration_seconds REAL,
  input_tokens INTEGER,
  output_tokens INTEGER,
  cache_read_tokens INTEGER,
  cache_write_tokens INTEGER,
  reasoning_tokens INTEGER,
  total_tokens INTEGER,
  num_turns INTEGER,
  ttft_ms REAL,
  api_duration_ms REAL,
  cost_usd REAL,
  usage_source TEXT NOT NULL CHECK (
    usage_source IN ('runtime_result', 'token_usage_updated', 'none')
  ),
  recorded_at REAL NOT NULL
);
