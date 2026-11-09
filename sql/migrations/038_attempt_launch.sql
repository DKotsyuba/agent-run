-- Launch correlation is never inferred for historical attempts. A spawning
-- row without a bound incarnation is unresolved until explicitly reconciled.
CREATE TABLE attempt_launches (
  spawn_key TEXT PRIMARY KEY NOT NULL,
  execution_id TEXT NOT NULL,
  sequence INTEGER NOT NULL CHECK (sequence > 0),
  attempt_id TEXT NOT NULL UNIQUE,
  launch_id TEXT NOT NULL UNIQUE CHECK (length(launch_id)=36),
  state TEXT NOT NULL CHECK (state IN ('prepared','spawning','bound','failed','unknown')),
  boot_id TEXT,
  pid_namespace_inode INTEGER,
  uid INTEGER,
  pid INTEGER,
  start_ticks INTEGER,
  CHECK ((state='bound' AND boot_id IS NOT NULL AND pid_namespace_inode > 0
          AND uid >= 0 AND pid > 1 AND start_ticks > 0)
      OR (state!='bound' AND boot_id IS NULL AND pid_namespace_inode IS NULL
          AND uid IS NULL AND pid IS NULL AND start_ticks IS NULL))
);

CREATE TRIGGER attempt_launch_identity_immutable BEFORE UPDATE ON attempt_launches
WHEN NEW.spawn_key IS NOT OLD.spawn_key OR NEW.execution_id IS NOT OLD.execution_id
  OR NEW.sequence IS NOT OLD.sequence OR NEW.attempt_id IS NOT OLD.attempt_id
  OR NEW.launch_id IS NOT OLD.launch_id OR OLD.state IN ('bound','failed')
  OR NOT ((OLD.state='prepared' AND NEW.state IN ('spawning','failed'))
       OR (OLD.state='spawning' AND NEW.state IN ('bound','failed','unknown'))
       OR (OLD.state='unknown' AND NEW.state IN ('bound','failed')))
BEGIN SELECT RAISE(ABORT, 'invalid attempt launch transition'); END;
