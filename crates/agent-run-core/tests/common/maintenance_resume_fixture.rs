//! Minimal already-allocated continuation; no provider or service is used.
use agent_run_core::{domain::AgentId, maintenance_execution::ResumeBoundary, state::Record};
use agent_run_store::Store;
use serde_json::json;

pub const SOURCE: &str = "resume-source";
pub const TARGET: &str = "resume-target";
pub const CONTROL: &str = "Resume using the saved checkpoint.";

/// One open fence and a single owned target attempt, before native input.
pub struct Fixture {
    pub home: super::common::Home,
    pub store: Store<'static>,
    pub run: AgentId,
    pub record: Record,
    pub boundary: ResumeBoundary,
}

impl Fixture {
    /// Select the source/target pair and optionally refuse the marker write.
    pub fn new(session: &str, reject_marker: bool) -> Self {
        let home = super::common::Home::new();
        let mut store = home.store();
        let mut request = home.request();
        request.explicit_finish = true;
        let (run, _) = store
            .admit(&request, &home.config, &json!({}), None)
            .unwrap();
        store
            .conn
            .execute(
                "UPDATE agents SET status='running',runtime_session_id=? WHERE id=?",
                rusqlite::params![session, run.as_str()],
            )
            .unwrap();
        store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,ownership_active) VALUES(?,?,1,'running','{}',1,1)",rusqlite::params![SOURCE,run.as_str()]).unwrap();
        let epoch = store
            .begin_maintenance_fence("resume-native-fixture", 2.0)
            .unwrap()
            .epoch;
        store.conn.execute("UPDATE attempts SET state='maintenance_paused',ownership_active=0,finished_at=3 WHERE id=?",[SOURCE]).unwrap();
        store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,ownership_active) VALUES(?,?,2,'running','{}',3,1)",rusqlite::params![TARGET,run.as_str()]).unwrap();
        store.conn.execute("INSERT INTO maintenance_resumes(epoch,agent_id,request_id,source_attempt_id,target_attempt_id,target_number,account_id,source_proof_sha256,writer_epoch,claimed_at,preflight_json) VALUES(?,?,?, ?,?,2,'fixture-account',?,1,3,'{}')",rusqlite::params![epoch,run.as_str(),"resume-native-request",SOURCE,TARGET,"0".repeat(64)]).unwrap();
        if reject_marker {
            store.conn.execute_batch("CREATE TRIGGER fixture_refuse_resume BEFORE INSERT ON events WHEN NEW.kind='maintenance_resume_confirmed_v1' BEGIN SELECT RAISE(ABORT,'fixture resume refusal'); END;").unwrap();
        }
        let mut record = store.get(&run).unwrap();
        record.resume_of_runtime_session_id = Some(session.into());
        let boundary = ResumeBoundary {
            epoch,
            source_attempt_id: SOURCE.into(),
            target_attempt_id: TARGET.into(),
        };
        Self {
            home,
            store,
            run,
            record,
            boundary,
        }
    }
}
