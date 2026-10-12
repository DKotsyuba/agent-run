//! `init` takes the exclusive offline authority before it writes anything: a
//! home held by another offline session (or a serving broker) is refused and
//! every file stays byte-for-byte what it was.
use std::{fs, path::Path};

/// Names and contents of every non-lock entry below `root`, sorted.
fn snapshot(root: &Path) -> Vec<(String, Vec<u8>)> {
    fn walk(dir: &Path, root: &Path, out: &mut Vec<(String, Vec<u8>)>) {
        let mut entries: Vec<_> = fs::read_dir(dir).unwrap().flatten().collect();
        entries.sort_by_key(|entry| entry.path());
        for entry in entries {
            let path = entry.path();
            let name = path.strip_prefix(root).unwrap().display().to_string();
            // Lock files carry no data; their existence is the exclusion itself.
            if name.ends_with(".lock") {
                continue;
            }
            if path.is_dir() {
                out.push((name, Vec::new()));
                walk(&path, root, out);
            } else {
                out.push((name, fs::read(&path).unwrap()));
            }
        }
    }
    let mut out = Vec::new();
    walk(root, root, &mut out);
    out
}

/// A busy home is refused before `config.toml`, `profiles/` or `skills/` are
/// written; once the holder is gone the same call initializes it.
#[test]
fn a_busy_home_is_refused_without_any_write() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("home");
    // Another offline session holds the home (the same exclusion a serving
    // broker's bind locks provide).
    fs::create_dir(&home).unwrap();
    fs::set_permissions(&home, std::os::unix::fs::PermissionsExt::from_mode(0o700)).unwrap();
    let holder = agent_run::offline::acquire(&home).expect("first session");
    let before = snapshot(&home);
    let refused = agent_run::init::initialize(&home);
    assert!(refused.is_err(), "a held home must be refused");
    assert_eq!(
        snapshot(&home),
        before,
        "a refused init must leave every file untouched"
    );
    assert!(!home.join("config.toml").exists());
    assert!(!home.join("profiles").exists());
    assert!(!home.join("skills").exists());
    drop(holder);
    agent_run::init::initialize(&home).expect("free home initializes");
    assert!(home.join("config.toml").exists());
}
