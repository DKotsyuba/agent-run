//! Synchronous SQLite scopes; transactions cannot escape into an async wait.
use rusqlite::Connection;
use std::sync::atomic::{AtomicU64, Ordering};

/// Unique trusted savepoint suffix; exhaustion refuses rather than wraps.
static NEXT_SCOPE: AtomicU64 = AtomicU64::new(1);

/// One top-level IMMEDIATE transaction or nested savepoint on the same connection.
/// Dropping an uncommitted scope rolls back only this scope; nested commit does
/// not imply durability. No external effect, I/O callback or await belongs here.
pub struct WriteScope<'connection> {
    /// Borrowed connection whose transaction/savepoint this scope owns.
    connection: &'connection Connection,
    /// Trusted generated savepoint name; None owns the outer transaction.
    savepoint: Option<String>,
    /// Commit/rollback already ended this scope.
    ended: bool,
}

impl<'connection> WriteScope<'connection> {
    /// Begin IMMEDIATE when autocommit, otherwise a fresh nested savepoint.
    /// SQLite acquire errors propagate; this never retries or changes timeouts.
    pub fn begin(connection: &'connection Connection) -> rusqlite::Result<Self> {
        let savepoint = if connection.is_autocommit() {
            connection.execute_batch("BEGIN IMMEDIATE")?;
            None
        } else {
            let id = NEXT_SCOPE
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
                .map_err(|_| rusqlite::Error::InvalidQuery)?;
            let name = format!("sqlite_access_{id}");
            connection.execute_batch(&format!("SAVEPOINT {name}"))?;
            Some(name)
        };
        Ok(Self {
            connection,
            savepoint,
            ended: false,
        })
    }

    /// Commit outer SQL or release nested savepoint; no callback/action replay.
    /// A failed commit still open is rolled back by Drop. An already-autocommit
    /// failure remains ambiguous and is returned without declaring rollback.
    pub fn commit(mut self) -> rusqlite::Result<()> {
        let result = match &self.savepoint {
            None => self.connection.execute_batch("COMMIT"),
            Some(name) => self.connection.execute_batch(&format!("RELEASE {name}")),
        };
        self.ended =
            result.is_ok() || (self.savepoint.is_none() && self.connection.is_autocommit());
        result
    }

    /// This scope owns an actual transaction rather than a nested savepoint.
    pub fn is_top_level(&self) -> bool {
        self.savepoint.is_none()
    }
}

impl std::ops::Deref for WriteScope<'_> {
    type Target = Connection;
    /// Borrow the same connection within the scope.
    fn deref(&self) -> &Connection {
        self.connection
    }
}

impl Drop for WriteScope<'_> {
    /// Roll back the owned unfinished scope; callers verify autocommit after failure.
    fn drop(&mut self) {
        if self.ended {
            return;
        }
        let _ = match &self.savepoint {
            None => self.connection.execute_batch("ROLLBACK"),
            Some(name) => self
                .connection
                .execute_batch(&format!("ROLLBACK TO {name}; RELEASE {name}")),
        };
    }
}

#[cfg(test)]
mod tests {
    //! A nested release must never independently commit its enclosing transaction.
    use super::WriteScope;
    use rusqlite::Connection;

    /// Nested success remains rollbackable by the owner; nested failure rolls
    /// back only its own rows and leaves the owner able to commit safely.
    #[test]
    fn nested_scopes_preserve_outer_atomicity() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch("CREATE TABLE rows(value INTEGER UNIQUE)")
            .unwrap();
        {
            let outer = WriteScope::begin(&connection).unwrap();
            outer.execute("INSERT INTO rows VALUES(1)", []).unwrap();
            let nested = WriteScope::begin(&outer).unwrap();
            assert!(!nested.is_top_level());
            nested.execute("INSERT INTO rows VALUES(2)", []).unwrap();
            nested.commit().unwrap();
            assert!(!connection.is_autocommit());
        }
        assert!(connection.is_autocommit());
        assert_eq!(
            connection
                .query_row("SELECT count(*) FROM rows", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            0
        );
        let outer = WriteScope::begin(&connection).unwrap();
        outer.execute("INSERT INTO rows VALUES(3)", []).unwrap();
        {
            let nested = WriteScope::begin(&outer).unwrap();
            nested.execute("INSERT INTO rows VALUES(4)", []).unwrap();
            assert!(nested.execute("INSERT INTO rows VALUES(4)", []).is_err());
        }
        outer.commit().unwrap();
        assert!(connection.is_autocommit());
        assert_eq!(
            connection
                .query_row("SELECT value FROM rows", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            3
        );
        assert_eq!(
            connection
                .query_row("SELECT count(*) FROM rows", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            1
        );
    }
}
