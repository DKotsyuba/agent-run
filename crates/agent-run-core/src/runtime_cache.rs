//! Bounded, best-effort CoW reuse of identical native plugin-cache files.
//! Each run retains its own inodes; no cache, history or credential path is shared.
use crate::{domain::AgentId, fs, state::Store, Result};
use agent_run_domain::AccountId;
use serde::Serialize;
use serde_json::Value;
use std::{
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

/// Logical bytes linked to existing APFS blocks; this is not reclaimed-space accounting.
#[derive(Default, Serialize)]
pub(crate) struct CacheReuse {
    /// Regular files considered before reaching the fixed traversal budget.
    pub files_examined: usize,
    /// Files replaced with independent, exact-byte clones.
    pub files_cloned: usize,
    /// Logical size of successfully cloned files, never a physical-savings estimate.
    pub bytes_cloned: usize,
    /// True when the entry, byte, depth or elapsed-time budget ended the pass.
    pub bounded: bool,
}

/// Reuses one completed same-account run's native plugin blocks after verified cleanup.
///
/// The caller exclusively owns an idle destination and has not committed its
/// terminal state yet. Only original, single-account source runs without a
/// continuation are candidates; paths must be immediate children of this
/// harness's configured runs root. Credentials, history, generated settings and
/// managed personal plugins are never traversed. Source disappearance, mutation
/// or unsupported cloning merely leaves the destination files independent.
/// No reference to the source is persisted, so ordinary retention remains valid.
pub(crate) fn reuse(
    store: &Store,
    id: &AgentId,
    account: &AccountId,
    runtime_home: &Path,
    harness_home: &Path,
) -> Result<CacheReuse> {
    if !cfg!(target_os = "macos") {
        return Ok(CacheReuse::default());
    }
    // ponytail: inspect only the newest 32 account attempts; miss a cache hit
    // rather than scanning unbounded history. Revisit only if measured hits suffer.
    let mut query = store.conn.prepare(
        "WITH recent AS (
           SELECT agent_id FROM attempts WHERE selected_account_id=?1
           ORDER BY rowid DESC LIMIT 32
         )
         SELECT a.identity_json FROM agents a JOIN recent r ON r.agent_id=a.id
         WHERE a.id<>?2 AND a.status='succeeded' AND a.parent_agent_id IS NULL
           AND NOT EXISTS (SELECT 1 FROM agents child WHERE child.parent_agent_id=a.id)
           AND NOT EXISTS (
             SELECT 1 FROM attempts t WHERE t.agent_id=a.id
               AND (t.selected_account_id IS NOT ?1 OR t.ownership_active<>0
                    OR t.phase IS NOT 'cleanup_complete')
           )
         ORDER BY a.created_at DESC LIMIT 8",
    )?;
    let candidates = query
        .query_map([account.as_str(), id.as_str()], |row| {
            row.get::<_, Option<String>>(0)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(query);
    let runs = harness_home.join("runs");
    for candidate in candidates {
        let Some(raw) = candidate else { continue };
        let Ok(identity) = serde_json::from_str::<Value>(&raw) else {
            continue;
        };
        if identity["authority"]["harness"].as_str() != Some("codex") {
            continue;
        }
        let Some(source) = identity["runtime_home"].as_str().map(PathBuf::from) else {
            continue;
        };
        let Ok(relative) = source.strip_prefix(&runs) else {
            continue;
        };
        if source == runtime_home
            || relative.components().count() != 1
            || relative
                .to_str()
                .and_then(|name| name.parse::<AgentId>().ok())
                .is_none()
        {
            continue;
        }
        let Ok(source) =
            fs::Dir::open(&source).and_then(|dir| dir.subdir(Path::new("plugins/cache")))
        else {
            continue;
        };
        // SAFETY: geteuid has no arguments or memory effects.
        if source.entry(None)?.uid != unsafe { libc::geteuid() } {
            continue;
        }
        let target = fs::Dir::open(runtime_home)?.subdir(Path::new("plugins/cache"))?;
        return deduplicate(&target, &source);
    }
    Ok(CacheReuse::default())
}

/// Compares at most 4096 entries / 128 MiB for two seconds, without following links.
///
/// A single file is capped at 16 MiB and directory depth at eight. Native cache
/// files which differ, disappear, or carry incompatible metadata are skipped.
/// The destination must be idle; source mutation is safe because every cloned
/// file is compared with the destination's captured bytes before publication.
fn deduplicate(target: &fs::Dir, source: &fs::Dir) -> Result<CacheReuse> {
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut result = CacheReuse::default();
    let mut stack = vec![(PathBuf::new(), 0_u8)];
    let mut entries = 0;
    let mut bytes = 0_u64;
    while let Some((relative, depth)) = stack.pop() {
        let directory = if relative.as_os_str().is_empty() {
            target.scan()
        } else {
            target.subdir(&relative)?.scan()
        };
        let Ok(mut directory) = directory else {
            continue;
        };
        loop {
            if entries >= 4096 || bytes >= 128 * 1024 * 1024 || Instant::now() >= deadline {
                result.bounded = true;
                return Ok(result);
            }
            let (names, done) = directory.next_batch(64)?;
            for name in names {
                entries += 1;
                if entries > 4096 || bytes >= 128 * 1024 * 1024 || Instant::now() >= deadline {
                    result.bounded = true;
                    return Ok(result);
                }
                if depth == 0 && name == "personal" {
                    continue;
                }
                let path = relative.join(name);
                match target.entry_type(&path) {
                    Ok(fs::EntryType::Directory) if depth < 8 => stack.push((path, depth + 1)),
                    Ok(fs::EntryType::Directory) => result.bounded = true,
                    Ok(fs::EntryType::File) => {
                        result.files_examined += 1;
                        let Ok(file) = target.open_file(&path) else {
                            continue;
                        };
                        let size = file.metadata()?.len();
                        bytes = bytes.saturating_add(size);
                        if size > 16 * 1024 * 1024 || bytes > 128 * 1024 * 1024 {
                            continue;
                        }
                        if let Ok(cloned) =
                            target.clone_matching_file(&path, source, &path, 16 * 1024 * 1024)
                        {
                            if cloned > 0 {
                                result.files_cloned += 1;
                                result.bytes_cloned += cloned;
                            }
                        }
                    }
                    _ => {}
                }
            }
            if done {
                break;
            }
        }
    }
    Ok(result)
}

#[cfg(all(test, target_os = "macos"))]
/// Exercises real-schema account/ownership selection and no-follow cache traversal.
mod tests {
    use super::*;
    use std::os::unix::fs::{symlink, MetadataExt};

    /// Inserts one minimal execution and cleanup attempt in the actual store schema.
    fn execution(
        store: &Store,
        runs: &Path,
        id: &str,
        account: &str,
        status: &str,
        at: f64,
    ) -> PathBuf {
        let home = runs.join(id);
        std::fs::create_dir_all(home.join("plugins/cache/remote/pkg/1")).unwrap();
        let identity = serde_json::json!({"authority":{"harness":"codex"},"runtime_home":home});
        store.conn.execute(
            "INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,
              request_json,status,created_at,timeout_seconds,config_revision,identity_json,root_agent_id)
             VALUES(?1,'codex','m','explore','fixture','fixture',?2,'{}',?3,?4,60,'fixture',?5,?1)",
            rusqlite::params![id, runs.to_string_lossy(), status, at, identity.to_string()],
        ).unwrap();
        store
            .conn
            .execute(
                "INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,
              selected_account_id,phase,ownership_active)
             VALUES(?1,?1,1,'completed','{}',?2,?3,'cleanup_complete',0)",
                rusqlite::params![id, at, account],
            )
            .unwrap();
        home
    }

    /// Only a completed same-account run provides bytes; settings and links remain untouched.
    #[cfg(target_os = "macos")]
    #[test]
    fn same_account_idle_cache_reuse_preserves_private_state() {
        let root = tempfile::tempdir().unwrap();
        let store = Store::initialize(root.path()).unwrap();
        let harness = root.path().join("codex");
        let runs = harness.join("runs");
        let source = execution(
            &store,
            &runs,
            "ag-20260928-180000-1111111111",
            "a",
            "succeeded",
            1.0,
        );
        execution(
            &store,
            &runs,
            "ag-20260928-180000-2222222222",
            "b",
            "succeeded",
            2.0,
        );
        execution(
            &store,
            &runs,
            "ag-20260928-180000-3333333333",
            "a",
            "running",
            3.0,
        );
        let id: AgentId = "ag-20260928-180000-4444444444".parse().unwrap();
        let target = execution(&store, &runs, id.as_str(), "a", "running", 4.0);
        let payload = vec![5_u8; 8192];
        let cache = Path::new("plugins/cache/remote/pkg/1");
        std::fs::write(source.join(cache).join("asset"), &payload).unwrap();
        std::fs::write(target.join(cache).join("asset"), &payload).unwrap();
        let secret = root.path().join("private");
        std::fs::write(&secret, "must remain private").unwrap();
        symlink(&secret, target.join(cache).join("link")).unwrap();
        symlink(&secret, source.join(cache).join("substituted")).unwrap();
        std::fs::write(
            target.join(cache).join("substituted"),
            "original cache data",
        )
        .unwrap();
        let settings = target.join("config.toml");
        std::fs::write(&settings, "private role").unwrap();
        std::fs::create_dir_all(target.join("plugins/cache/personal")).unwrap();
        std::fs::write(target.join("plugins/cache/personal/asset"), &payload).unwrap();
        std::fs::create_dir_all(source.join("plugins/cache/personal")).unwrap();
        std::fs::write(source.join("plugins/cache/personal/asset"), &payload).unwrap();
        let personal_inode = std::fs::metadata(target.join("plugins/cache/personal/asset"))
            .unwrap()
            .ino();
        let report = reuse(&store, &id, &"a".parse().unwrap(), &target, &harness).unwrap();
        assert_eq!(report.files_cloned, 1);
        assert_eq!(report.bytes_cloned, payload.len());
        assert_eq!(std::fs::read_to_string(&settings).unwrap(), "private role");
        assert_eq!(
            std::fs::read_to_string(&secret).unwrap(),
            "must remain private"
        );
        assert!(target.join(cache).join("link").is_symlink());
        assert_eq!(
            std::fs::read_to_string(target.join(cache).join("substituted")).unwrap(),
            "original cache data"
        );
        assert_eq!(
            std::fs::metadata(target.join("plugins/cache/personal/asset"))
                .unwrap()
                .ino(),
            personal_inode
        );
        std::fs::remove_dir_all(source).unwrap();
        assert_eq!(
            std::fs::read(target.join(cache).join("asset")).unwrap(),
            payload
        );
    }

    /// Owned, foreign-account and continued homes cannot be selected as idle originals.
    #[test]
    fn cache_source_requires_matching_account_and_confirmed_idle_original() {
        let root = tempfile::tempdir().unwrap();
        let store = Store::initialize(root.path()).unwrap();
        let harness = root.path().join("codex");
        let runs = harness.join("runs");
        let source_id = "ag-20260928-180000-1111111111";
        let source = execution(&store, &runs, source_id, "b", "succeeded", 1.0);
        let id: AgentId = "ag-20260928-180000-2222222222".parse().unwrap();
        let target = execution(&store, &runs, id.as_str(), "a", "running", 2.0);
        for home in [&source, &target] {
            std::fs::write(
                home.join("plugins/cache/remote/pkg/1/asset"),
                vec![7_u8; 8192],
            )
            .unwrap();
        }
        assert_eq!(
            reuse(&store, &id, &"a".parse().unwrap(), &target, &harness)
                .unwrap()
                .files_examined,
            0
        );
        let source_id = "ag-20260928-180000-3333333333";
        execution(&store, &runs, source_id, "a", "succeeded", 1.5);
        store
            .conn
            .execute(
                "UPDATE attempts SET ownership_active=1 WHERE agent_id=?1",
                [source_id],
            )
            .unwrap();
        assert_eq!(
            reuse(&store, &id, &"a".parse().unwrap(), &target, &harness)
                .unwrap()
                .files_examined,
            0
        );
        store
            .conn
            .execute(
                "UPDATE attempts SET ownership_active=0 WHERE agent_id=?1",
                [source_id],
            )
            .unwrap();
        store
            .conn
            .execute(
                "UPDATE agents SET parent_agent_id=?1 WHERE id=?2",
                [source_id, id.as_str()],
            )
            .unwrap();
        assert_eq!(
            reuse(&store, &id, &"a".parse().unwrap(), &target, &harness)
                .unwrap()
                .files_examined,
            0
        );
    }
}
