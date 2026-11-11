//! Synthetic Quota service observed by the real fixture supervisor.
use super::*;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixListener,
    task::JoinHandle,
};

const SNAPSHOT: &str =
    include_str!("../../../agent-run-core/tests/fixtures/quota-client-v1/snapshot-v2.json");

/// One owned fake Quota socket; it only answers snapshot reads.
struct FakeQuota {
    _directory: tempfile::TempDir,
    path: std::path::PathBuf,
    task: Option<JoinHandle<()>>,
    requests: Arc<Mutex<Vec<Value>>>,
}

impl FakeQuota {
    /// Starts a private listener under the configured short socket root.
    fn start(provider: &'static str, accounts: [&'static str; 2]) -> Self {
        let base = std::env::var_os("AGENT_RUN_TEST_SOCKET_TMP")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let directory = tempfile::Builder::new()
            .prefix("qa-switch-")
            .tempdir_in(base)
            .unwrap();
        let path = directory.path().join("quota.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let observed = requests.clone();
        let task = tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let mut request = Vec::new();
                if stream.read_to_end(&mut request).await.is_err() {
                    continue;
                }
                if let Ok(value) = serde_json::from_slice(&request) {
                    observed.lock().unwrap().push(value);
                }
                let at = agent_run_core::domain::now();
                let mut bytes = serde_json::to_vec(&response(at, provider, accounts)).unwrap();
                bytes.push(b'\n');
                let _ = stream.write_all(&bytes).await;
            }
        });
        Self {
            _directory: directory,
            path,
            task: Some(task),
            requests,
        }
    }

    /// Stops and joins this test's listener before removing its directory.
    async fn stop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
    }
}

impl Drop for FakeQuota {
    /// Prevents a failed assertion from leaving the owned listener alive.
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// Produces two current known accounts, each with a separate physical pool.
fn response(at: f64, provider: &str, account_ids: [&str; 2]) -> Value {
    let until = at + 300.0;
    let mut base: Value = serde_json::from_str(SNAPSHOT).unwrap();
    let mut accounts = Vec::new();
    for id in account_ids {
        let source: Value = serde_json::from_str(&SNAPSHOT.replace("acct-test", id)).unwrap();
        let mut account = source["result"]["accounts"][0].clone();
        account["models"][0]["provider"] = json!(provider);
        account["models"][0]["model"] = json!("fixture");
        account["models"][0]["native_model"] = json!("fixture");
        account["models"][0]["state_validity"] = json!({"kind":"until","at":until});
        account["models"][0]["reset_at"] = json!(until + 1000.0);
        account["models"][0]["newest_observed_at"] = json!(at);
        account["windows"][0]["runtime"] = json!(provider);
        account["windows"][0]["models"] = json!(["fixture"]);
        account["windows"][0]["observed_at"] = json!(at);
        account["windows"][0]["valid_until"] = json!(until);
        account["windows"][0]["last_reset_at"] = json!(until + 1000.0);
        account["windows"][0]["forecast"]["observed_at"] = json!(at);
        account["windows"][0]["forecast"]["reset_at"] = json!(until + 1000.0);
        account["windows"][0]["evaluation"]["reset_at"] = json!(until + 1000.0);
        account["advice"][0]["reset_at"] = json!(until + 1000.0);
        accounts.push(account);
    }
    base["result"]["evaluated_at"] = json!(at);
    base["result"]["accounts"] = json!(accounts);
    base["result"]["coverage"]["last_collection"]["observed_at"] = json!(at);
    base["result"]["coverage"]["last_collection"]["sources"][0]["account"] = json!(account_ids[0]);
    base["result"]["coverage"]["last_collection"]["sources"][0]["provider"] = json!(provider);
    base
}

/// Configures only the disposable home's Quota endpoint.
fn configure(home: &Path, socket: &Path) {
    let config_path = home.join("config.toml");
    let mut config: toml::Value =
        toml::from_str(&fs::read_to_string(&config_path).unwrap()).unwrap();
    let root = config.as_table_mut().unwrap();
    let capacity = root
        .entry("capacity")
        .or_insert_with(|| toml::Value::Table(Default::default()));
    capacity.as_table_mut().unwrap().insert(
        "quota".into(),
        toml::Value::Table(
            [(
                "socket".into(),
                toml::Value::String(socket.to_string_lossy().into_owned()),
            )]
            .into_iter()
            .collect(),
        ),
    );
    fs::write(&config_path, toml::to_string(&config).unwrap()).unwrap();
}

/// The switch reads Quota again but never sends native-failure feedback to it.
#[tokio::test]
async fn quota_v2_supervisor_failover_uses_a_fresh_read_without_feedback() {
    let mut quota = FakeQuota::start("codex-user", ["acct-cx-a", "acct-cx-b"]);
    let (_temp, home) = codex_home(["exhausted", "ok"]);
    configure(&home, &quota.path);
    let id = codex_run(&home, "quota-v2-switch", None).await;
    let store = agent_run_store::fixture::open(&home).unwrap();
    assert_eq!(store.get(&id).unwrap().status, Status::Succeeded);
    let attempts = attempts(&home, &id);
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0].1, "acct-cx-a");
    assert_eq!(attempts[1].1, "acct-cx-b");
    let refs: Vec<String> = store.conn.prepare(
        "SELECT data_json FROM events WHERE agent_id=? AND kind IN ('start_accepted','attempt_allocated') ORDER BY seq"
    ).unwrap().query_map([id.as_str()], |row| row.get(0)).unwrap().collect::<rusqlite::Result<_>>().unwrap();
    assert_eq!(refs.len(), 2);
    for data in refs {
        let data: Value = serde_json::from_str(&data).unwrap();
        assert_eq!(data["quota_decision"]["revision"], 2);
    }
    let requests = quota.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 2);
    assert!(requests.iter().all(|request| request["version"] == 1
        && request["method"] == "snapshot"
        && request["accounts"] == json!([])
        && request.get("observation").is_none()));
    quota.stop().await;
}

/// Ordered-provider cooldown remains local even when Quota still reports both accounts known.
#[tokio::test]
async fn quota_v2_ordered_cooldown_survives_fresh_quota_reads() {
    let mut quota = FakeQuota::start("glm-user", ["acct-work", "acct-sub"]);
    let (_temp, home) = ordered_claude_home();
    configure(&home, &quota.path);
    let service = fixture_service(home.clone()).await;
    let mut input = request(&home);
    input.task = "fixture:quota".into();
    input.account = None;
    input.request_id = Some("quota-v2-ordered".into());
    let first = service.admit_provider(input.clone()).await.unwrap();
    let id: AgentId = serde_json::from_value(first["agent_id"].clone()).unwrap();
    run_to_end(&home, &id).await;
    let observed = attempts(&home, &id);
    assert_eq!(observed.len(), 2);
    assert_eq!(observed[0].1, "acct-work");
    assert_eq!(observed[1].1, "acct-sub");
    let store = agent_run_store::fixture::open(&home).unwrap();
    let reason: String = store.conn.query_row(
        "SELECT reason FROM binding_cooldowns WHERE provider_id='glm-user' AND account_id='acct-work'",
        [], |row| row.get(0),
    ).unwrap();
    assert_eq!(reason, "quota_exhausted");
    input.task = "fixture:answer".into();
    input.request_id = Some("quota-v2-ordered-next".into());
    let next = service.admit_provider(input).await.unwrap();
    let next_id: AgentId = serde_json::from_value(next["agent_id"].clone()).unwrap();
    assert_eq!(attempts(&home, &next_id)[0].1, "acct-sub");
    let requests = quota.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 3);
    assert!(
        requests
            .iter()
            .all(|request| request["method"] == "snapshot" && request.get("observation").is_none())
    );
    quota.stop().await;
}
