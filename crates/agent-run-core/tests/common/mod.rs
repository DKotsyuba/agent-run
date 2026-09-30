#![allow(dead_code)]
use agent_run_config::config::Config;
use agent_run_domain::domain::StartRequest;
use agent_run_platform::fs;
use agent_run_store::Store;
use serde_json::json;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

/// One valid read-only resolved-role payload with its self-consistent
/// canonical config revision.
pub fn role_payload() -> serde_json::Value {
    let mut payload = json!({
        "role_name": "review",
        "role_revision": "fixture",
        "prompt": "Review the fixture.",
        "grants": {
            "write": false,
            "network": false,
            "allow_external_read_roots": false,
            "read_roots": [],
        },
        "skills": [],
        "mcp": [],
        "required_constraints": [],
        "auth": {"mode": "global", "reference": null},
    });
    let revision = agent_run_domain::canonical::sha256_hex(&payload, true);
    payload["config_revision"] = json!(revision);
    payload
}

/// One minimal frozen launch plan whose only qualification inputs are the
/// harness binary and its environment, for driving the real guard
/// qualification body with an isolated fixture harness.
pub fn launch_plan(binary: &Path) -> agent_run_adapters::provider::ProviderLaunchPlan {
    let role = agent_run_config::role_plan::ResolvedRolePlan::from_payload(&role_payload())
        .expect("role payload");
    agent_run_adapters::provider::ProviderLaunchPlan {
        launch: agent_run_adapters::LaunchPlan {
            binary: binary.to_path_buf(),
            args: vec!["app-server".into()],
            cwd: std::env::temp_dir(),
            environment: Default::default(),
            initial_input: None,
        },
        native_model: "fixture".into(),
        role,
        runtime: serde_json::from_value(json!({
            "enabled": true, "adapter": "codex", "binary": "/usr/bin/true",
            "home": "/tmp/agent-run-fixture-runtime",
            "models": ["fixture"],
        }))
        .expect("runtime"),
        profile: agent_run_config::profiles::Profile {
            name: "review".into(),
            body: "Review the fixture.".into(),
            write: false,
            network: false,
            revision: "fixture".into(),
            canonical: false,
            allow_external_read_roots: true,
            read_roots: vec![],
            skills: vec![],
            mcp: vec![],
            mcp_tools: Default::default(),
            required_constraints: Default::default(),
        },
    }
}

pub struct Home {
    pub temp: TempDir,
    pub path: PathBuf,
    pub config: Config,
}
impl Home {
    pub fn new() -> Self {
        let temp = tempfile::tempdir().expect("temporary directory");
        let path = temp.path().canonicalize().unwrap();
        fs::private_dir(&path).unwrap();
        let binary = if Path::new("/usr/bin/true").is_file() {
            "/usr/bin/true"
        } else {
            "/bin/true"
        };
        let text = format!("schema_version=1\n[runtimes.mock]\nenabled=true\nadapter='claude'\nbinary={}\nhome={}\nmodels=['fixture']\nlimits_source='none'\n", toml::Value::String(binary.into()), toml::Value::String(path.join("runtimes/mock").to_string_lossy().into_owned()));
        std::fs::write(path.join("config.toml"), text).unwrap();
        let config = Config::load(&path).unwrap();
        Store::initialize(&path).unwrap();
        Self { temp, path, config }
    }
    pub fn request(&self) -> StartRequest {
        let mut request: StartRequest = serde_json::from_value(json!({"runtime":"mock", "model":"fixture", "profile":"review", "task":"fixture task", "workdir":self.path})).unwrap();
        request.validate().unwrap();
        request
    }
    pub fn store(&self) -> Store {
        Store::open(&self.path).unwrap()
    }
}

/// Name stem of the measured native curated-clone Git pack.
pub const PACK_STEM: &str = "pack-7e4ca65304fb7a4c26cb519fa403dc19da3c543c";
/// Measured byte length of the native curated-clone `.pack` file.
pub const MEASURED_PACK_BYTES: u64 = 24_205_581;

/// Shape of one synthetic native curated plugin clone (`.tmp/plugins`).
///
/// `plugins` top-level plugin directories each hold `dirs_per_plugin`
/// directories (at least 10: a nine-deep chain below the plugin, so the
/// working tree reaches depth 10, plus siblings); `files` regular files of
/// `file_bytes` bytes each are spread round-robin over every directory with
/// distinct content. `pack_bytes` sizes the sparse `.pack` file; the `.idx`
/// and `.rev` files carry the measured sizes with deterministic bytes.
pub struct CuratedShape {
    pub plugins: usize,
    pub dirs_per_plugin: usize,
    pub files: usize,
    pub file_bytes: usize,
    pub pack_bytes: u64,
}

/// The measured native working tree: 5380 files and 2352 directories
/// (7732 entries), depth 10; `file_bytes`/`pack_bytes` are set per test.
pub fn measured_shape(file_bytes: usize, pack_bytes: u64) -> CuratedShape {
    CuratedShape {
        plugins: 49,
        dirs_per_plugin: 48,
        files: 5380,
        file_bytes,
        pack_bytes,
    }
}

/// Writes one native curated plugin clone below `home/.tmp`.
///
/// The working tree `plugins/` follows `shape`; `.git` carries the pack
/// directory plus private per-home state (HEAD, config, index, logs, refs)
/// whose bytes embed `private_tag`, and the clone's `.agents`, root files and
/// the `.tmp/plugins.sha`/`.tmp/plugins.sync.lock` siblings are written too.
/// Identical shapes produce byte-identical working trees and packs, so two
/// homes differ only in their private Git state. Returns the private paths
/// that must never be shared, rewritten or replaced.
pub fn curated_clone(home: &Path, shape: &CuratedShape, private_tag: &str) -> Vec<PathBuf> {
    use std::fs as stdfs;
    let clone = home.join(".tmp/plugins");
    let tree = clone.join("plugins");
    let mut directories = Vec::new();
    for plugin in 0..shape.plugins {
        let mut chain = tree.join(format!("plugin-{plugin:02}"));
        directories.push(chain.clone());
        for depth in 1..10 {
            chain = chain.join(format!("level-{depth}"));
            directories.push(chain.clone());
        }
        for sibling in 10..shape.dirs_per_plugin.max(10) {
            directories.push(tree.join(format!("plugin-{plugin:02}/extra-{sibling:02}")));
        }
    }
    for directory in &directories {
        stdfs::create_dir_all(directory).expect("curated directory");
    }
    for index in 0..shape.files {
        let directory = &directories[index % directories.len()];
        let seed = format!("curated file {index}\n");
        let body: Vec<u8> = seed.bytes().cycle().take(shape.file_bytes.max(1)).collect();
        stdfs::write(directory.join(format!("file-{index:05}.md")), body).expect("curated file");
    }
    let pack = clone.join(".git/objects/pack");
    stdfs::create_dir_all(&pack).expect("pack dir");
    let packfile = stdfs::File::create(pack.join(format!("{PACK_STEM}.pack"))).expect("pack");
    packfile.set_len(shape.pack_bytes).expect("sparse pack");
    for (extension, bytes) in [("idx", 204_856_usize), ("rev", 29_164)] {
        let body: Vec<u8> = (0..bytes).map(|index| (index % 251) as u8).collect();
        stdfs::write(pack.join(format!("{PACK_STEM}.{extension}")), body).expect("pack index");
    }
    let private = [
        (
            ".tmp/plugins/.git/HEAD",
            "ref: refs/heads/main\n".to_owned(),
        ),
        (
            ".tmp/plugins/.git/config",
            format!("[core]\n\t# {private_tag}\n"),
        ),
        (".tmp/plugins/.git/index", format!("DIRC {private_tag}\n")),
        (
            ".tmp/plugins/.git/logs/HEAD",
            format!("clone {private_tag}\n"),
        ),
        (
            ".tmp/plugins/.git/refs/heads/main",
            format!("{private_tag}\n"),
        ),
        (
            ".tmp/plugins/.agents/plugins/marketplace.json",
            "{}\n".to_owned(),
        ),
        (".tmp/plugins/.gitignore", "*.tmp\n".to_owned()),
        (".tmp/plugins/README.md", "# curated\n".to_owned()),
        (".tmp/plugins.sha", format!("{private_tag}\n")),
        (".tmp/plugins.sync.lock", String::new()),
    ];
    private
        .iter()
        .map(|(relative, body)| {
            let path = home.join(relative);
            stdfs::create_dir_all(path.parent().expect("parent")).expect("private parent");
            stdfs::write(&path, body).expect("private file");
            path
        })
        .collect()
}

/// Exact identity of private files: path, bytes, inode and modification time.
pub fn private_identity(paths: &[PathBuf]) -> Vec<(PathBuf, Vec<u8>, u64, i64)> {
    use std::os::unix::fs::MetadataExt;
    paths
        .iter()
        .map(|path| {
            let metadata = std::fs::symlink_metadata(path).expect("private metadata");
            assert!(
                metadata.is_file(),
                "private state stays a real file: {path:?}"
            );
            (
                path.clone(),
                std::fs::read(path).expect("private bytes"),
                metadata.ino(),
                metadata.mtime_nsec() + metadata.mtime() * 1_000_000_000,
            )
        })
        .collect()
}

/// A small curated clone (2 plugins, 20 directories, 40 files of 64 bytes,
/// depth 10) with a `pack_bytes`-long sparse pack, for fast behavior tests.
pub fn curated_shape_small(pack_bytes: u64) -> CuratedShape {
    CuratedShape {
        plugins: 2,
        dirs_per_plugin: 10,
        files: 40,
        file_bytes: 64,
        pack_bytes,
    }
}
