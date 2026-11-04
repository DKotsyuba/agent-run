//! Local read-only discovery and execution-fenced sealed answer reads.

use crate::{Error, Result, agent_identity, service::Service, state::Record, verify};
use agent_run_domain::{
    Sha256Digest,
    domain::AgentId,
    error::invalid,
    observation::{AnswerChunkQuery, AnswerChunkView},
};
use agent_run_store::writer::StoreReader;
use serde_json::{Value, json};

/// Reports compiled product facts and the authoritative supported read schemas.
/// No configuration, provider, process, checkout Git or database is consulted.
/// Source revision stays null because this build has no verified provenance input.
pub fn capabilities() -> Value {
    json!({
        "schema_version": 1,
        "product": "agent-run",
        "version": env!("CARGO_PKG_VERSION"),
        "source_revision": null,
        "transports": ["cli", "mcp", "unix_socket"],
        "limits": {
            "socket_max_frame_bytes": agent_run_domain::observation::SOCKET_MAX_FRAME_BYTES,
            "answer_max_bytes": verify::MAX_ANSWER,
            "answer_chunk_default_bytes": agent_run_domain::observation::DEFAULT_CHUNK_BYTES,
            "answer_chunk_max_bytes": agent_run_domain::observation::MAX_CHUNK_BYTES
        },
        "read_methods": agent_run_domain::registry().iter()
            .filter(|tool| tool.annotations.read_only_hint)
            .map(|tool| json!({"name":tool.name,"input_schema":tool.input_schema,
                "result_shape":tool.output_schema.as_ref().or(tool.result_shape.as_ref())})).collect::<Vec<_>>()
    })
}

/// Builds answer proof exclusively from the broker's exact execution record.
/// Missing size/hash is corrupt evidence; no path means an unavailable answer.
pub(crate) fn answer_proof(row: &Record) -> Result<verify::Proof> {
    Ok(verify::Proof {
        path: row.answer_path.clone().ok_or(Error::AnswerUnavailable)?,
        bytes: row
            .answer_bytes
            .ok_or_else(|| Error::Integrity("answer size evidence is missing".into()))?,
        sha256: row
            .answer_sha256
            .clone()
            .ok_or_else(|| Error::Integrity("answer digest is missing".into()))?,
        proof_version: 2,
    })
}

/// Fingerprints the exact execution and recorded proof without accepting a path from a client.
fn cli_proof_token(row: &Record, proof: &verify::Proof, version: u32) -> Result<String> {
    let material = serde_json::to_vec(&(
        &row.root_agent_id,
        &row.id,
        row.sequence,
        &proof.path,
        proof.bytes,
        &proof.sha256,
        version,
    ))?;
    Ok(crate::fs::sha256(&material))
}

impl Service {
    /// Resolves one CLI answer selection once and returns verified metadata plus
    /// an opaque proof fence. Inline bytes are retrieved separately in bounded pages.
    pub(crate) fn cli_answer(&self, agent_id: &AgentId, run_id: Option<&AgentId>) -> Result<Value> {
        let store = StoreReader::open(&self.home)?;
        let row = agent_identity::resolve(&store, agent_id, run_id)?;
        let mut answer = agent_identity::result(&row, self.answer(&row.id)?)?;
        let current = agent_identity::resolve_sequence(&store, &row.root_agent_id, row.sequence)?;
        if current.id != row.id {
            return Err(Error::ExecutionChanged);
        }
        if current.answer_path != row.answer_path
            || current.answer_bytes != row.answer_bytes
            || current.answer_sha256 != row.answer_sha256
        {
            return Err(Error::AnswerIntegrity(
                "answer proof changed during read".into(),
            ));
        }
        let inline = answer["inline_complete"]
            .as_bool()
            .ok_or_else(|| Error::AnswerIntegrity("invalid answer metadata".into()))?;
        if inline != answer["content"].is_string() {
            return Err(Error::AnswerIntegrity("invalid answer metadata".into()));
        }
        let token = if row.answer_path.is_some() {
            let proof = answer_proof(&row)?;
            if answer["available"] != true
                || answer["path"] != json!(&proof.path)
                || answer["size_bytes"] != json!(proof.bytes)
                || answer["sha256"] != json!(&proof.sha256)
            {
                return Err(Error::AnswerIntegrity(
                    "answer proof changed during read".into(),
                ));
            }
            let version = answer["proof_version"]
                .as_u64()
                .and_then(|version| u32::try_from(version).ok())
                .ok_or_else(|| Error::AnswerIntegrity("invalid answer proof version".into()))?;
            Some(cli_proof_token(&row, &proof, version)?)
        } else {
            if answer["available"] != false || inline {
                return Err(Error::AnswerIntegrity(
                    "answer proof changed during read".into(),
                ));
            }
            None
        };
        answer["content"] = Value::Null;
        answer["inline_complete"] = Value::Bool(false);
        Ok(json!({"answer":answer,"sequence":row.sequence,"inline":inline,"proof_token":token}))
    }

    /// Reads one bounded exact-artifact chunk without changing state or launching
    /// work. Both sequence and hash fence the current execution. The complete
    /// artifact is verified before slicing, then current execution and recorded
    /// proof are rechecked. UTF-8 offsets, beyond-EOF cursors, and budgets that
    /// cannot fit the next character are validation errors. Returns empty at EOF.
    pub fn answer_chunk(&self, query: &AnswerChunkQuery) -> Result<AnswerChunkView> {
        self.answer_chunk_observed(query, || {})
    }

    /// Shares the real read path with a deterministic test observer invoked
    /// after artifact verification and before the current-record recheck.
    /// Production supplies a no-op; the observer never receives answer content.
    fn answer_chunk_observed(
        &self,
        query: &AnswerChunkQuery,
        after_read: impl FnOnce(),
    ) -> Result<AnswerChunkView> {
        self.answer_chunk_with_fence(query, None, after_read)
    }

    /// Reads a retained exact execution with its original proof fence, even if
    /// a newer execution has since appeared in the same lineage.
    pub(crate) fn cli_answer_chunk(
        &self,
        query: &AnswerChunkQuery,
        proof_token: &Sha256Digest,
    ) -> Result<AnswerChunkView> {
        self.answer_chunk_with_fence(query, Some(proof_token.as_str()), || {})
    }

    /// Verifies a full artifact and slices it, rechecking either the current tip
    /// or an exact retained execution according to the optional CLI proof fence.
    fn answer_chunk_with_fence(
        &self,
        query: &AnswerChunkQuery,
        proof_token: Option<&str>,
        after_read: impl FnOnce(),
    ) -> Result<AnswerChunkView> {
        query.validate()?;
        let store = StoreReader::open(&self.home)?;
        let row = if proof_token.is_some() {
            agent_identity::resolve_sequence(&store, &query.agent_id, query.sequence.get())?
        } else {
            agent_identity::resolve(&store, &query.agent_id, None)?
        };
        if row.sequence != query.sequence.get() {
            return Err(Error::ExecutionChanged);
        }
        let proof = answer_proof(&row)?;
        if proof.sha256 != query.expected_sha256.as_str() {
            return Err(Error::AnswerHashMismatch);
        }
        let root = self.home.join("agents").join(row.id.as_str());
        let (version, text) = verify::read_payload(&root, &proof)?;
        if let Some(expected) = proof_token
            && cli_proof_token(&row, &proof, version)? != expected
        {
            return Err(Error::AnswerIntegrity(
                "answer proof changed during read".into(),
            ));
        }
        after_read();
        let offset = usize::try_from(query.offset_bytes)
            .map_err(|_| invalid("offset_bytes exceeds answer size"))?;
        if offset > text.len() || !text.is_char_boundary(offset) {
            return Err(invalid(
                "offset_bytes must be a UTF-8 boundary within the answer",
            ));
        }
        let mut end = offset.saturating_add(query.max_bytes).min(text.len());
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        if end == offset && offset < text.len() {
            return Err(invalid("max_bytes cannot fit the next UTF-8 character"));
        }
        let current = if proof_token.is_some() {
            agent_identity::resolve_sequence(&store, &query.agent_id, query.sequence.get())?
        } else {
            agent_identity::resolve(&store, &query.agent_id, None)?
        };
        if current.id != row.id || current.sequence != row.sequence {
            return Err(Error::ExecutionChanged);
        }
        let current_proof = answer_proof(&current)?;
        if current_proof.sha256 != proof.sha256 {
            return Err(Error::AnswerHashMismatch);
        }
        if current_proof.path != proof.path || current_proof.bytes != proof.bytes {
            return Err(Error::AnswerIntegrity(
                "answer proof changed during read".into(),
            ));
        }
        Ok(AnswerChunkView {
            agent_id: row.root_agent_id,
            sequence: row.sequence,
            sha256: proof.sha256,
            total_bytes: proof.bytes,
            offset_bytes: query.offset_bytes,
            next_offset: end as u64,
            content: text[offset..end].to_owned(),
            complete: end == text.len(),
        })
    }
}

#[cfg(test)]
/// Deterministic current-execution and proof revalidation tests.
mod tests {
    use super::*;
    use std::path::Path;

    /// A resume or proof replacement after verification must reject the entire
    /// response. The hook fixes the exact interleaving without timing guesses.
    #[test]
    fn answer_chunk_rechecks_execution_and_proof_after_read() {
        for resume in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let home = temp.path().canonicalize().unwrap();
            crate::fs::private_dir(&home).unwrap();
            std::fs::write(home.join("config.toml"), format!("schema_version=1\n[runtimes.mock]\nenabled=true\nadapter='claude'\nbinary='/usr/bin/true'\nhome={}\nmodels=['fixture']\nlimits_source='none'\n",toml::Value::String(home.join("runtimes/mock").to_string_lossy().into_owned()))).unwrap();
            let config = crate::config::Config::load(&home).unwrap();
            agent_run_store::fixture::initialize(&home).unwrap();
            let mut store = agent_run_store::fixture::open(&home).unwrap();
            let request = serde_json::from_value(json!({"runtime":"mock","model":"fixture","profile":"review","task":"synthetic","workdir":home})).unwrap();
            let (id, _) = store.admit(&request, &config, &json!({}), None).unwrap();
            let root = home.join("agents").join(id.as_str());
            crate::fs::private_dir(&root).unwrap();
            let proof = verify::seal(&root, Path::new("answer.md"), "first answer").unwrap();
            store
                .conn
                .execute(
                    "UPDATE agents SET answer_path=?1,answer_bytes=?2,answer_sha256=?3 WHERE id=?4",
                    rusqlite::params![
                        proof.path.to_str().unwrap(),
                        proof.bytes,
                        proof.sha256,
                        id.as_str()
                    ],
                )
                .unwrap();
            let child = if resume {
                Some(store.admit(&request, &config, &json!({}), None).unwrap().0)
            } else {
                None
            };
            let query = serde_json::from_value(json!({"agent_id":id,"sequence":1,
                "expected_sha256":proof.sha256,"offset_bytes":0}))
            .unwrap();
            let error = Service::new(home.clone()).answer_chunk_observed(&query, || {
                if let Some(child) = &child {
                    store.conn.execute("UPDATE agents SET root_agent_id=?1,parent_agent_id=?1,sequence=2 WHERE id=?2",
                        rusqlite::params![id.as_str(),child.as_str()]).unwrap();
                } else {
                    store.conn.execute("UPDATE agents SET answer_sha256=?1 WHERE id=?2",
                        rusqlite::params!["0".repeat(64),id.as_str()]).unwrap();
                }
            }).unwrap_err();
            assert!(
                if resume {
                    matches!(error, Error::ExecutionChanged)
                } else {
                    matches!(error, Error::AnswerHashMismatch)
                },
                "{error:?}"
            );
        }
    }
}
