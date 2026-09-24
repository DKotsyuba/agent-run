//! Global account registration and status changes over schema v17 facts.

use crate::Store;
use agent_run_domain::{
    catalog::{AccountId, AccountRecord, AccountStatus, AuthFamily, SecretRef},
    error::invalid,
    CredentialRef, Result,
};
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use std::str::FromStr;

/// Decodes one trusted registry row into validated public identity types.
pub(crate) fn account_record(
    (id, family, reference, status): (String, String, String, String),
) -> Result<AccountRecord> {
    Ok(AccountRecord {
        account_id: AccountId::from_str(&id)?,
        auth_family: AuthFamily::from_str(&family)?,
        secret_ref: SecretRef::from_str(&reference)?,
        status: AccountStatus::from_str(&status)?,
    })
}

/// Reads account references in stable id order through an existing connection, without opening or migrating a store.
/// This also supports read-only planning against schema 17 before a schema upgrade.
pub fn list_at(connection: &rusqlite::Connection) -> Result<Vec<AccountRecord>> {
    let mut statement=connection.prepare("SELECT account_id,auth_family,secret_ref,status FROM provider_accounts ORDER BY account_id")?;
    let rows = statement
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    rows.into_iter().map(account_record).collect()
}

impl Store {
    /// Registers one global account with an existing, typed credential
    /// reference. Neither credentials nor provider-local labels are stored.
    ///
    /// Duplicate ids or canonical auth-family/reference identities fail
    /// without changing previous rows. A successful registration advances
    /// the quota capacity revision in the same transaction, so advice
    /// computed before it (`models`, `capacity_order`, candidate sets) is
    /// visibly stale.
    pub fn register_account(&mut self, record: &AccountRecord) -> Result<()> {
        if !CredentialRef::from_secret(&record.secret_ref)?.matches_family(&record.auth_family) {
            return Err(invalid("native credential family does not match account"));
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = tx.execute(
            "INSERT INTO provider_accounts(account_id,auth_family,secret_ref,status,created_at,updated_at) \
             VALUES (?1,?2,?3,?4,?5,?5)",
            params![
                record.account_id.as_str(),
                record.auth_family.as_str(),
                record.secret_ref.as_str(),
                record.status.as_str(),
                agent_run_domain::domain::now(),
            ],
        );
        match result {
            Ok(_) => {
                Store::advance_quota_capacity_revision(&tx)?;
                tx.commit()?
            }
            Err(error)
                if error.sqlite_error_code() == Some(rusqlite::ErrorCode::ConstraintViolation) =>
            {
                return Err(invalid(
                    "account id or credential reference is already registered",
                ));
            }
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }

    /// Returns the registered metadata for `id`, or `None` when absent.
    /// References name storage locations and contain no credential bytes.
    pub fn account(&self, id: &AccountId) -> Result<Option<AccountRecord>> {
        let row: Option<(String, String, String, String)> = self.conn.query_row(
            "SELECT account_id,auth_family,secret_ref,status FROM provider_accounts WHERE account_id=?",
            [id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        ).optional()?;
        row.map(account_record).transpose()
    }

    /// Lists all registered accounts in global-id order, including disabled
    /// records whose past attempt history remains readable.
    pub fn list_accounts(&self) -> Result<Vec<AccountRecord>> {
        list_at(&self.conn)
    }

    /// Disables future selection for an existing account without deleting
    /// its reference, reservations, attempts, or historical messages.
    ///
    /// The status change and a quota capacity revision advance commit in one
    /// transaction, so availability can never change under an unchanged
    /// advertised revision. An unknown id changes nothing.
    pub fn disable_account(&mut self, id: &AccountId) -> Result<()> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = tx.execute(
            "UPDATE provider_accounts SET status='disabled',updated_at=? WHERE account_id=?",
            params![agent_run_domain::domain::now(), id.as_str()],
        )?;
        if changed == 0 {
            return Err(invalid("account is not registered"));
        }
        Store::advance_quota_capacity_revision(&tx)?;
        tx.commit()?;
        Ok(())
    }
}
