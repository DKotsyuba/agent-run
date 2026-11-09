//! Linux operator-socket boundary tests with synthetic same-UID clients.
//! Real supplementary-group admission and cross-UID denial require a root-owned
//! native test namespace; these tests never switch users or launch model work.
#![cfg(target_os = "linux")]

use agent_run::{cli, transport::socket};
use serde_json::{Value, json};
use std::{
    fs,
    os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
};

/// Owns the isolated home and a protected operator directory on the prepared filesystem.
struct Fixture {
    /// Cleanup owner for the protected socket directory.
    directory: tempfile::TempDir,
    /// Cleanup owner for the synthetic database and home.
    home_directory: tempfile::TempDir,
    /// Initialized synthetic broker home.
    home: PathBuf,
    /// Explicit group-authorized endpoint.
    operator: PathBuf,
    /// Current group used only for same-UID synthetic coverage.
    gid: u32,
}

impl Fixture {
    /// Separates the protected socket root from project-owned fixture data.
    fn new() -> Self {
        let base = std::env::var_os("AGENT_RUN_OPERATOR_SOCKET_TMP")
            .or_else(|| std::env::var_os("AGENT_RUN_TEST_SOCKET_TMP"))
            .or_else(|| std::env::var_os("TMPDIR"))
            .expect("prepare an owned socket test directory");
        let directory = tempfile::Builder::new()
            .prefix("op-")
            .tempdir_in(base)
            .unwrap();
        let home_base = std::env::var_os("AGENT_RUN_TEST_TMP")
            .or_else(|| std::env::var_os("TMPDIR"))
            .expect("prepare an owned test-data directory");
        let home_directory = tempfile::Builder::new()
            .prefix("op-home-")
            .tempdir_in(home_base)
            .unwrap();
        // Only this newly created fixture wrapper is made searchable. The
        // prepared root and every pre-existing home/ancestor remain untouched.
        // SAFETY: getegid only reads the current process identity.
        let gid = unsafe { libc::getegid() };
        std::os::unix::fs::chown(directory.path(), None, Some(gid)).unwrap();
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o750)).unwrap();
        let home = home_directory.path().join("home");
        configure(&home);
        let parent = directory.path().join("operator");
        fs::create_dir(&parent).unwrap();
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o750)).unwrap();
        std::os::unix::fs::chown(&parent, None, Some(gid)).unwrap();
        Self {
            directory,
            home_directory,
            home,
            operator: parent.join("api.sock"),
            gid,
        }
    }

    /// Starts the explicit two-listener server and waits for both public pings.
    async fn start(&self, options: socket::ServeOptions) -> Running {
        let (home, operator, gid) = (self.home.clone(), self.operator.clone(), self.gid);
        let running = Running(tokio::spawn(async move {
            socket::serve_with_operator(&home, &operator, gid, options).await
        }));
        ready(&self.home.join("api.sock"), &running).await;
        ready(&self.operator, &running).await;
        running
    }
}

/// Configures only the local transport, without launching a harness or accessing credentials.
fn configure(home: &Path) {
    cli::init(home).unwrap();
    fs::write(home.join("config.toml"), format!(
        "schema_version=1\n[runtimes.mock]\nenabled=true\nadapter='claude'\nbinary='/bin/true'\nhome='{}'\nmodels=['fixture']\nlimits_source='none'\n",
        home.join("mock").display()
    )).unwrap();
}

/// Cancels the owned broker if a test assertion unwinds.
struct Running(tokio::task::JoinHandle<agent_run::Result<()>>);

impl Running {
    /// Joins cancellation, then observes both API guards and the writer guard release.
    async fn stop(&mut self, fixture: &Fixture) {
        self.0.abort();
        let _ = (&mut self.0).await;
        tokio::time::timeout(Duration::from_secs(5), async {
            while fixture.home.join("api.sock").exists()
                || fixture.home.join("writer.sock").exists()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("broker guards release after cancellation");
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Performs one bounded real LF-framed request, leaving UID policy to the server.
async fn request(path: &Path, method: &str) -> Value {
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut stream = UnixStream::connect(path).await.unwrap();
        let mut bytes =
            serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":{}}))
                .unwrap();
        bytes.push(b'\n');
        // An overloaded listener may reply and close before reading a frame.
        // Its complete error envelope still proves the observed admission refusal.
        let written = stream.write_all(&bytes).await;
        let mut line = String::new();
        BufReader::new(stream).read_line(&mut line).await.unwrap();
        assert!(
            !line.is_empty(),
            "no response after request write: {written:?}"
        );
        serde_json::from_str(&line).unwrap()
    })
    .await
    .expect("bounded operator request")
}

/// Waits for binding and a successful ping, failing promptly if startup is rejected.
async fn ready(path: &Path, running: &Running) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !path.exists() {
            assert!(
                !running.0.is_finished(),
                "broker rejected fixture; use root/service-owned ancestors and no ACLs"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        loop {
            let response = request(path, "ping").await;
            if response["result"] == json!({"ok":true}) {
                break;
            }
            assert_eq!(response["error"]["code"], -32001);
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("broker startup deadline");
}

/// The default still binds only private sockets and retains its worker protocol surface.
#[tokio::test]
async fn default_private_api_remains_private() {
    let fixture = Fixture::new();
    let home = fixture.home.clone();
    let mut running = Running(tokio::spawn(async move { socket::serve(&home).await }));
    ready(&fixture.home.join("api.sock"), &running).await;
    for name in ["api.sock", "writer.sock"] {
        assert_eq!(
            fs::metadata(fixture.home.join(name)).unwrap().mode() & 0o7777,
            0o600
        );
    }
    assert!(!fixture.operator.exists());
    assert_ne!(
        request(&fixture.home.join("api.sock"), "worker/call").await["error"]["code"],
        -32601
    );
    running.stop(&fixture).await;
}

/// Publication has final ownership and mode; the group endpoint rejects every private worker method.
#[tokio::test]
async fn operator_publication_and_private_method_fence() {
    let fixture = Fixture::new();
    let mut running = fixture.start(socket::ServeOptions::default()).await;
    let metadata = fs::symlink_metadata(&fixture.operator).unwrap();
    // SAFETY: geteuid only reads the current process identity.
    let uid = unsafe { libc::geteuid() };
    assert_eq!(metadata.uid(), uid);
    assert_eq!(metadata.gid(), fixture.gid);
    assert_eq!(metadata.mode() & 0o7777, 0o660);
    assert!(metadata.file_type().is_socket());
    for method in [
        "worker/notify",
        "worker/call",
        "worker/catalog",
        "worker/future",
    ] {
        assert_eq!(
            request(&fixture.operator, method).await["error"]["code"],
            -32601
        );
    }
    assert!(
        request(&fixture.operator, "tools")
            .await
            .get("result")
            .is_some()
    );
    for name in ["api.sock", "writer.sock"] {
        assert_eq!(
            fs::metadata(fixture.home.join(name)).unwrap().mode() & 0o7777,
            0o600
        );
    }
    assert!(
        fs::read_dir(fixture.operator.parent().unwrap())
            .unwrap()
            .all(|entry| {
                !entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".op-")
            })
    );
    running.stop(&fixture).await;
    assert!(!fixture.operator.exists());
}

/// Unsafe paths and permissions refuse before either public endpoint is created.
#[tokio::test]
async fn rejects_unsafe_directories_and_malformed_configuration() {
    let fixture = Fixture::new();
    let directory = fixture.operator.parent().unwrap();
    for mode in [0o700, 0o770, 0o751, 0o2750] {
        fs::set_permissions(directory, fs::Permissions::from_mode(mode)).unwrap();
        assert!(
            socket::serve_with_operator(
                &fixture.home,
                &fixture.operator,
                fixture.gid,
                socket::ServeOptions::default()
            )
            .await
            .is_err()
        );
        assert!(!fixture.home.join("api.sock").exists());
    }
    fs::set_permissions(directory, fs::Permissions::from_mode(0o750)).unwrap();
    for (path, gid) in [
        (Path::new("relative.sock"), fixture.gid),
        (fixture.operator.as_path(), u32::MAX),
    ] {
        assert!(
            socket::serve_with_operator(&fixture.home, path, gid, socket::ServeOptions::default())
                .await
                .is_err()
        );
    }
    let alias = fixture.directory.path().join("alias");
    std::os::unix::fs::symlink(directory, &alias).unwrap();
    assert!(
        socket::serve_with_operator(
            &fixture.home,
            &alias.join("api.sock"),
            fixture.gid,
            socket::ServeOptions::default()
        )
        .await
        .is_err()
    );
    fs::set_permissions(fixture.directory.path(), fs::Permissions::from_mode(0o770)).unwrap();
    assert!(
        socket::serve_with_operator(
            &fixture.home,
            &fixture.operator,
            fixture.gid,
            socket::ServeOptions::default()
        )
        .await
        .is_err()
    );
}

/// Neither an existing live endpoint nor a replaced inode is removed by another broker/guard.
#[tokio::test]
async fn operator_lock_and_guard_preserve_existing_entries() {
    let fixture = Fixture::new();
    let mut running = fixture.start(socket::ServeOptions::default()).await;
    let other_home = fixture.home_directory.path().join("other");
    configure(&other_home);
    assert!(
        socket::serve_with_operator(
            &other_home,
            &fixture.operator,
            fixture.gid,
            socket::ServeOptions::default()
        )
        .await
        .is_err()
    );
    assert_eq!(
        request(&fixture.operator, "ping").await["result"],
        json!({"ok":true})
    );
    fs::remove_file(&fixture.operator).unwrap();
    fs::write(&fixture.operator, "replacement").unwrap();
    running.stop(&fixture).await;
    assert_eq!(
        fs::read_to_string(&fixture.operator).unwrap(),
        "replacement"
    );
}

/// Both public listeners spend the same ordinary connection permit.
#[tokio::test]
async fn operator_and_private_share_connection_capacity() {
    let fixture = Fixture::new();
    let mut running = fixture
        .start(socket::ServeOptions {
            max_connections: 1,
            ..socket::ServeOptions::default()
        })
        .await;
    let held = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let mut held = UnixStream::connect(fixture.home.join("api.sock"))
                .await
                .unwrap();
            let _ = held
                .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}\n")
                .await;
            let mut held = BufReader::new(held);
            let mut line = String::new();
            held.read_line(&mut line).await.unwrap();
            let response: Value = serde_json::from_str(&line).unwrap();
            if response["result"] == json!({"ok":true}) {
                break held;
            }
            assert_eq!(response["error"]["code"], -32001);
        }
    })
    .await
    .expect("obtain the ordinary connection permit");
    assert_eq!(
        request(&fixture.operator, "ping").await["error"]["code"],
        -32001
    );
    drop(held);
    running.stop(&fixture).await;
}

/// Installs a synthetic POSIX ACL; no credential or host directory is touched.
fn add_acl(path: &Path, default: bool) {
    use std::{ffi::CString, os::unix::ffi::OsStrExt};
    let mut bytes = 2u32.to_le_bytes().to_vec();
    // Extended ACL keeps mode 0750 but grants a named unrelated UID traversal.
    for (tag, permissions, id) in [
        (1u16, 7u16, u32::MAX),
        (2, 5, 123456),
        (4, 5, u32::MAX),
        (16, 5, u32::MAX),
        (32, 0, u32::MAX),
    ] {
        bytes.extend(tag.to_le_bytes());
        bytes.extend(permissions.to_le_bytes());
        bytes.extend(id.to_le_bytes());
    }
    let path = CString::new(path.as_os_str().as_bytes()).unwrap();
    let name = if default {
        c"system.posix_acl_default"
    } else {
        c"system.posix_acl_access"
    };
    // SAFETY: all buffers outlive setxattr and their lengths/types match the Linux ACL wire encoding.
    let result = unsafe {
        libc::setxattr(
            path.as_ptr(),
            name.as_ptr(),
            bytes.as_ptr().cast(),
            bytes.len(),
            0,
        )
    };
    assert_eq!(
        result,
        0,
        "fixture filesystem must support POSIX ACLs: {}",
        std::io::Error::last_os_error()
    );
}

/// Access and inherited default ACLs cannot silently broaden the configured operator group.
#[tokio::test]
async fn operator_directory_acls_are_refused() {
    for default in [false, true] {
        let fixture = Fixture::new();
        add_acl(fixture.operator.parent().unwrap(), default);
        assert!(
            socket::serve_with_operator(
                &fixture.home,
                &fixture.operator,
                fixture.gid,
                socket::ServeOptions::default()
            )
            .await
            .is_err()
        );
        assert!(!fixture.operator.exists());
        assert!(!fixture.home.join("api.sock").exists());
    }
}

/// A secure but owner-only ancestor cannot silently strand all other operators.
#[tokio::test]
async fn operator_nontraversable_ancestor_is_refused() {
    let fixture = Fixture::new();
    fs::set_permissions(fixture.directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let error = socket::serve_with_operator(
        &fixture.home,
        &fixture.operator,
        fixture.gid,
        socket::ServeOptions::default(),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("cannot traverse"));
    assert!(!fixture.operator.exists());
    assert!(!fixture.home.join("api.sock").exists());
}

/// An ancestor access ACL can override mode-class traversal for a named user.
#[tokio::test]
async fn operator_ancestor_access_acl_is_refused() {
    let fixture = Fixture::new();
    add_acl(fixture.directory.path(), false);
    let error = socket::serve_with_operator(
        &fixture.home,
        &fixture.operator,
        fixture.gid,
        socket::ServeOptions::default(),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("ACL"));
    assert!(!fixture.operator.exists());
    assert!(!fixture.home.join("api.sock").exists());
}
