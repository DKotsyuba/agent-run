//! Local account and group checks for a worker temporary directory.

use agent_run_domain::{Result, error::invalid};
use std::{collections::BTreeSet, ffi::CStr, mem::MaybeUninit, ptr};

/// Trusted UIDs resolved from the local account database for one path check.
pub struct TempAuthority {
    users: BTreeSet<u32>,
    groups: BTreeSet<u32>,
}

/// Explicit operator assertion about every principal that can use a group.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum TrustedGroup {
    Name(String),
    Gid(u32),
}

impl TempAuthority {
    /// Resolves configured account names and always trusts the executing UID.
    pub fn new(accounts: &[String], groups: &[TrustedGroup]) -> Result<Self> {
        // SAFETY: geteuid only reads process credentials.
        let mut users = BTreeSet::from([unsafe { libc::geteuid() }]);
        for account in accounts {
            let name = std::ffi::CString::new(account.as_str())
                .map_err(|_| invalid("invalid worker_temp trusted account name"))?;
            let uid = account_uid(&name).ok_or_else(|| {
                invalid(format!(
                    "worker_temp trusted account {account:?} could not be resolved"
                ))
            })?;
            users.insert(uid);
        }
        let groups = groups
            .iter()
            .map(|group| match group {
                TrustedGroup::Gid(gid) if *gid != u32::MAX => Ok(*gid),
                TrustedGroup::Name(name) => {
                    let name = std::ffi::CString::new(name.as_str())
                        .map_err(|_| invalid("invalid worker_temp trusted group name"))?;
                    group_gid(&name)
                        .ok_or_else(|| invalid("worker_temp trusted group could not be resolved"))
                }
                TrustedGroup::Gid(_) => Err(invalid("invalid worker_temp trusted GID")),
            })
            .collect::<Result<_>>()?;
        Ok(Self { users, groups })
    }

    /// Checks a named user or a directory owner's UID.
    pub(super) fn trusts_uid(&self, uid: u32) -> bool {
        self.users.contains(&uid)
    }

    /// Accepts only GIDs explicitly trusted by the operator.
    pub(super) fn trusts_group(&self, gid: u32) -> bool {
        self.groups.contains(&gid)
    }
}

/// Resolves one account through the reentrant POSIX account lookup.
fn account_uid(name: &CStr) -> Option<u32> {
    let mut buffer = vec![0u8; 65536];
    let mut account = MaybeUninit::<libc::passwd>::uninit();
    let mut found = ptr::null_mut();
    // SAFETY: the input name and all output storage are valid for the call.
    let code = unsafe {
        libc::getpwnam_r(
            name.as_ptr(),
            account.as_mut_ptr(),
            buffer.as_mut_ptr().cast(),
            buffer.len(),
            &mut found,
        )
    };
    if code != 0 || found.is_null() {
        return None;
    }
    // SAFETY: successful lookup initialized account and returned it in found.
    Some(unsafe { account.assume_init() }.pw_uid)
}

/// Resolves a group name without attempting to enumerate its members.
fn group_gid(name: &CStr) -> Option<u32> {
    let mut buffer = vec![0u8; 65536];
    let mut group = MaybeUninit::<libc::group>::uninit();
    let mut found = ptr::null_mut();
    // SAFETY: the input and output storage are valid for the duration of the call.
    let code = unsafe {
        libc::getgrnam_r(
            name.as_ptr(),
            group.as_mut_ptr(),
            buffer.as_mut_ptr().cast(),
            buffer.len(),
            &mut found,
        )
    };
    if code != 0 || found.is_null() {
        return None;
    }
    // SAFETY: successful lookup initialized group.
    Some(unsafe { group.assume_init() }.gr_gid)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Trust originates exclusively in the configured name or numeric GID.
    #[test]
    fn worker_temp_group_assertions_are_explicit() {
        let empty = TempAuthority::new(&[], &[]).unwrap();
        assert!(!empty.trusts_group(0));
        let numeric = TempAuthority::new(&[], &[TrustedGroup::Gid(1005)]).unwrap();
        assert!(numeric.trusts_group(1005));
        assert!(!numeric.trusts_group(0));
        assert!(
            TempAuthority::new(
                &[],
                &[TrustedGroup::Name("agent-run-missing-group-91f3".into())]
            )
            .is_err()
        );
    }
}
