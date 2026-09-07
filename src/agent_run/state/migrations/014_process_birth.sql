ALTER TABLE agents ADD COLUMN supervisor_birth_time REAL;
ALTER TABLE agents ADD COLUMN startup_owner_birth_time REAL;
ALTER TABLE workflow_runs ADD COLUMN owner_birth_time REAL;
