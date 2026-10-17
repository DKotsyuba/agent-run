-- Keep stale producer-watermark probes indexed while admission is fenced.
CREATE INDEX operation_receipts_producer ON operation_receipts(producer_key);
