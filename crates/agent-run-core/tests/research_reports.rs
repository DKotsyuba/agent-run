//! Real shared dispatcher and filesystem boundary checks; no engine or model runs.
use agent_run_core::{dispatch, service::Service, state::Store};
use agent_run_domain::{
    catalog::{AccountRecord, AccountStatus},
    domain::AgentId,
    worker::{TOOL_METHOD, WorkerToolCall},
};
use serde_json::{Value, json};
use std::{fs, os::unix::fs::symlink, path::PathBuf};

/// Disposable role/account/attempt state with a real authenticated worker token.
struct Fixture {
    /// Owns all files for this finite, process-free fixture.
    _temp: tempfile::TempDir,
    /// Broker home, containing only synthetic config and token hashes.
    home: PathBuf,
    /// Exact assigned report directory.
    reports: PathBuf,
    /// Admitted execution, kept immutable across calls.
    agent: AgentId,
    /// Its ownership-active attempt.
    attempt: String,
    /// Synthetic worker bearer, never a production credential.
    token: String,
}
impl Fixture {
    /// Admits a restricted research or ordinary review role using an inert
    /// executable, then marks the synthetic attempt running without spawning.
    fn new(research: bool) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().canonicalize().unwrap();
        let reports = home.join("reports");
        fs::create_dir_all(&reports).unwrap();
        fs::create_dir_all(home.join("profiles")).unwrap();
        let mut store = Store::initialize(&home).unwrap();
        let constraints = if research {
            "[\"research_tools_only\"]"
        } else {
            "[]"
        };
        fs::write(home.join("profiles/role.md"), format!("+++\nrevision=\"1\"\nwrite=false\nnetwork={research}\nallow_external_read_roots=false\nskills=[]\nmcp=[]\nrequired_constraints={constraints}\n+++\nResearch with sources.\n")).unwrap();
        fs::write(home.join("config.toml"), format!("schema_version=2\n[harnesses.codex]\nbinary=\"/usr/bin/true\"\nhome=\"{}/codex\"\n[harnesses.claude-code]\nbinary=\"/usr/bin/true\"\nhome=\"/tmp\"\n[providers.claude]\nharness=\"claude-code\"\nconnection={{kind=\"native\"}}\nauth_family=\"anthropic\"\nlimits_source=\"none\"\n[[providers.claude.models]]\nid=\"fixture\"\n[[providers.claude.bindings]]\nlabel=\"work\"\naccount=\"acct\"\n", home.display())).unwrap();
        store
            .register_account(&AccountRecord {
                account_id: "acct".parse().unwrap(),
                auth_family: "anthropic".parse().unwrap(),
                secret_ref: "native:claude-code".parse().unwrap(),
                status: AccountStatus::Enabled,
            })
            .unwrap();
        let request = serde_json::from_value(json!({"provider":"claude","model":"fixture","profile":"role","task":"Save a report","workdir":reports})).unwrap();
        let admitted = Service::new(home.clone()).admit_provider(request).unwrap();
        let agent: AgentId = serde_json::from_value(admitted["agent_id"].clone()).unwrap();
        let attempt: String = store
            .conn
            .query_row(
                "SELECT id FROM attempts WHERE agent_id=?",
                [agent.as_str()],
                |row| row.get(0),
            )
            .unwrap();
        store
            .conn
            .execute(
                "UPDATE agents SET status='running' WHERE id=?",
                [agent.as_str()],
            )
            .unwrap();
        store
            .conn
            .execute("UPDATE attempts SET state='running' WHERE id=?", [&attempt])
            .unwrap();
        let token = "a".repeat(64);
        store
            .issue_worker_capability(&agent, &attempt, &token, agent_run_core::domain::now())
            .unwrap();
        Self {
            _temp: temp,
            home,
            reports,
            agent,
            attempt,
            token,
        }
    }
    /// Calls the real private broker route with fixed hidden capability fields.
    async fn save(&self, filename: &str, content: &str) -> agent_run_core::Result<Value> {
        dispatch::call(
            &Service::new(self.home.clone()),
            TOOL_METHOD,
            serde_json::to_value(WorkerToolCall {
                run_id: self.agent.clone(),
                attempt_id: self.attempt.clone(),
                token: self.token.clone(),
                tool: "save_report".into(),
                input: json!({"filename":filename,"content":content}),
            })
            .unwrap(),
        )
        .await
    }
}

/// Publication and duplicate replay preserve exact bytes; overwrites, path
/// traversal, absolute paths and symlink escapes never modify existing data.
#[tokio::test]
async fn research_report_dispatch_enforces_directory_and_create_only_boundary() {
    let f = Fixture::new(true);
    let text = "Primary source: https://example.com/docs\n";
    let receipt = f.save("report.md", text).await.unwrap();
    assert_eq!(
        receipt["sha256"],
        agent_run_core::fs::sha256(text.as_bytes())
    );
    assert_eq!(
        fs::read_to_string(f.reports.join("report.md")).unwrap(),
        text
    );
    assert_eq!(f.save("report.md", text).await.unwrap()["duplicate"], true);
    assert!(f.save("report.md", "overwrite").await.is_err());
    let outside = f.home.join("outside.md");
    fs::write(&outside, "preserve").unwrap();
    symlink(&outside, f.reports.join("linked.md")).unwrap();
    for name in [
        "../outside.md",
        "/tmp/outside.md",
        "sub/report.md",
        "linked.md",
        ".codex/config.json",
        "script.sh",
    ] {
        assert!(f.save(name, "escape").await.is_err(), "{name}");
    }
    assert_eq!(fs::read_to_string(&outside).unwrap(), "preserve");
    assert_eq!(
        fs::read_to_string(f.reports.join("report.md")).unwrap(),
        text
    );
    assert!(!f.reports.join("sub").exists());
}

/// The tool cannot grant writing to another role or to a stopped attempt.
#[tokio::test]
async fn research_report_requires_live_research_authority() {
    let ordinary = Fixture::new(false);
    assert!(ordinary.save("report.md", "not authorized").await.is_err());
    assert!(!ordinary.reports.join("report.md").exists());
    let f = Fixture::new(true);
    Store::open(&f.home)
        .unwrap()
        .conn
        .execute(
            "UPDATE agents SET status='cancelling' WHERE id=?",
            [f.agent.as_str()],
        )
        .unwrap();
    assert!(f.save("report.md", "late").await.is_err());
    assert!(!f.reports.join("report.md").exists());
}
