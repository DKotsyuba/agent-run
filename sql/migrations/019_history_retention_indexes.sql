-- Bound foreign-key checks when pruning expired attempts and their event journals.
-- The migration runner prepares auto_vacuum=INCREMENTAL before this transaction.
CREATE INDEX idx_events_attempt ON events(attempt_id) WHERE attempt_id IS NOT NULL;
CREATE INDEX idx_messages_attempt ON messages(attempt_id) WHERE attempt_id IS NOT NULL;
CREATE INDEX idx_deliveries_terminal_event ON deliveries(terminal_event_seq) WHERE terminal_event_seq IS NOT NULL;
