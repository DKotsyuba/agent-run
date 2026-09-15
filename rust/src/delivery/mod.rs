//! Durable completion outbox. No task, answer, credentials, or provider prose in notices.
pub mod relay;
use crate::{config::Config,domain::{now,AgentId,Status},error::invalid,fs,state::Store,Result};
use rusqlite::{params,OptionalExtension,TransactionBehavior};
use serde::{Deserialize,Serialize};
use serde_json::{json,Value};
use std::{path::Path,time::Duration};

#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct Notice{pub notification_id:String,pub agent_id:AgentId,pub status:Status,pub runtime:Option<String>,pub model:Option<String>,pub effort:Option<String>,pub failure_kind:Option<String>}
fn escaped(value:Option<&str>,missing:&str)->String{
    let mut result=String::new();for c in value.unwrap_or(missing).chars().take(128){if c.is_control()||c=='\u{2028}'||c=='\u{2029}'{result.push_str(&format!("\\u{:04x}",c as u32));}else{result.push(c);}}result
}
fn guidance(kind:Option<&str>,status:Status)->(&'static str,&'static str){
    if status==Status::TimedOut{return("Legacy runtime deadline was reached.","Inspect the transcript before deciding whether to resume.");}
    match kind{
        Some("provider_overloaded")=>("The provider is temporarily overloaded.","Retry later or choose another compatible runtime/model."),
        Some("quota_exhausted"|"rate_limited")=>("The provider quota or rate limit blocked the run.","Inspect limits and select a fresh compatible route."),
        Some("auth_error")=>("Runtime authentication was rejected.","Check the configured account with agent-run doctor."),
        _ if status==Status::Lost=>("The recorded supervisor no longer owns a live process.","Inspect answer and transcript; do not assume execution succeeded."),
        _=>("The run did not produce a verified successful outcome.","Inspect answer and transcript before taking any further action."),
    }
}
impl Notice {
    pub fn validate(&self)->Result<()> {
        if !self.status.terminal()||!self.notification_id.starts_with("ntf_")||self.notification_id.len()<=4||self.notification_id.len()>512||!self.notification_id[4..].bytes().all(|c|c.is_ascii_alphanumeric()||b"_-".contains(&c)){return Err(invalid("invalid completion lifecycle fields"));}
        for value in [&self.runtime,&self.model,&self.effort,&self.failure_kind].into_iter().flatten(){if value.trim().is_empty()||value.chars().count()>128{return Err(invalid("invalid completion metadata"));}}
        Ok(())
    }
    pub fn render(&self)->Result<String>{
        self.validate()?;let mut text=format!("agent-run/completion\n- ID: {}\n- Status: {}",self.agent_id,self.status.as_str());
        if matches!(self.status,Status::Failed|Status::Lost|Status::TimedOut){let(reason,advice)=guidance(self.failure_kind.as_deref(),self.status);text.push_str(&format!("\n- Failure: {reason}\n- Advice: {advice}"));}
        text.push_str(&format!("\n- Runtime: {}/{}:{}\n- Notification: {}\n",escaped(self.runtime.as_deref(),"unknown"),escaped(self.model.as_deref(),"unknown"),escaped(self.effort.as_deref(),"unspecified"),self.notification_id));Ok(text)
    }
}
#[derive(Debug,Clone,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Evidence {pub classifier:String,pub duration_ms:u64,pub accepted:bool,pub ambiguous:bool}
impl Evidence{
    fn new(classifier:&str,accepted:bool,ambiguous:bool)->Self{Self{classifier:classifier.into(),duration_ms:0,accepted,ambiguous}}
}
pub fn safe_evidence(raw:&Value)->Option<Evidence>{
    let e:Evidence=serde_json::from_value(raw.clone()).ok()?;
    if !["relay_accepted","relay_rejected","relay_unavailable","relay_ambiguous","uds_written","session_gone","uds_unavailable","uds_ambiguous","unsupported_transport","delivery_expired"].contains(&e.classifier.as_str()){return None;}Some(e)
}
struct Claim{delivery_id:String,lease:String,attempt:u32,transport:String,session:String,notice:Notice}
fn claim(home:&Path)->Result<Option<Claim>> {
    let mut store=Store::open(home)?;let tx=store.conn.transaction_with_behavior(TransactionBehavior::Immediate)?;let time=now();
    let found=tx.query_row("SELECT id,agent_id,orchestrator_session_id,attempts FROM deliveries WHERE (state IN ('pending','retry_wait') AND COALESCE(next_attempt_at,0)<=?) OR (state='sending' AND lease_until<=?) ORDER BY COALESCE(next_attempt_at,0),id LIMIT 1",params![time,time],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,u32>(3)?))).optional()?;
    let Some((delivery_id,agent_id,sid,attempt))=found else{return Ok(None);};
    let(transport,session)=tx.query_row("SELECT transport,external_session_id FROM orchestrator_sessions WHERE id=?",[sid],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?)))?;
    let (runtime,model,raw):(String,String,String)=tx.query_row("SELECT runtime,model,request_json FROM agents WHERE id=?",[&agent_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
    // Historical request JSON may not satisfy the current request schema.
    // Delivery needs only immutable selectors, never the task or credentials.
    let effort=serde_json::from_str::<Value>(&raw).ok().and_then(|v|v.get("effort").and_then(Value::as_str).map(str::to_owned));
    let(status,kind):(String,Option<String>)=tx.query_row("SELECT status,failure_kind FROM agents WHERE id=?",[&agent_id],|r|Ok((r.get(0)?,r.get(1)?)))?;
    let ntf=if delivery_id.starts_with("ntf_"){delivery_id.clone()}else{format!("ntf_{}",&fs::sha256(delivery_id.as_bytes())[..32])};
    let bounded=|v:String|if !v.trim().is_empty()&&v.chars().count()<=128{Some(v)}else{None};
    let notice=Notice{notification_id:ntf,agent_id:agent_id.parse()?,status:status.parse()?,runtime:bounded(runtime),model:bounded(model),effort:effort.and_then(bounded),failure_kind:kind.and_then(bounded)};notice.validate()?;
    let lease=uuid::Uuid::new_v4().to_string();let attempt=attempt.checked_add(1).ok_or_else(||invalid("delivery attempt counter overflow"))?;
    tx.execute("UPDATE deliveries SET state='sending',attempts=?,lease_owner=?,lease_until=? WHERE id=?",params![attempt,lease,time+30.,delivery_id])?;tx.commit()?;
    Ok(Some(Claim{delivery_id,lease,attempt,transport,session,notice}))
}
fn complete(home:&Path,claim:&Claim,evidence:&Evidence)->Result<()> {
    let cfg=Config::load(home)?;let mut store=Store::open(home)?;let tx=store.conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let owns:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM deliveries WHERE id=? AND state='sending' AND lease_owner=? AND attempts=?)",params![claim.delivery_id,claim.lease,claim.attempt],|r|r.get(0))?;
    if !owns{return Ok(());}
    let time=now(); // Bound notices retry indefinitely when max_attempts is zero, as upstream does.
    let exhausted=cfg.delivery.max_attempts>0&&claim.attempt>=cfg.delivery.max_attempts;
    let state=if evidence.accepted{"delivered"}else if exhausted||evidence.classifier=="unsupported_transport"{"failed"}else{"retry_wait"};
    let backoff=(cfg.delivery.retry_base_seconds*2f64.powi((claim.attempt.saturating_sub(1)).min(24) as i32)).min(cfg.delivery.retry_cap_seconds);
    // Outcome and immutable evidence commit together under the same owned lease.
    tx.execute("INSERT INTO delivery_attempt_evidence(delivery_id,attempt,recorded_at,evidence_json) VALUES(?,?,?,?)",params![claim.delivery_id,claim.attempt,time,serde_json::to_string(evidence)?])?;
    tx.execute("UPDATE deliveries SET state=?,lease_owner=NULL,lease_until=NULL,next_attempt_at=?,last_error=?,ambiguous_result=? WHERE id=?",params![state,time+backoff,if evidence.accepted{None}else{Some(evidence.classifier.as_str())},evidence.ambiguous,claim.delivery_id])?;tx.commit()?;Ok(())
}
pub async fn dispatch_once(home:&Path)->Result<usize>{
    let Some(claim)=claim(home)?else{return Ok(0);};let started=std::time::Instant::now();
    let mut evidence=match claim.transport.as_str(){
        "codex_queue"=>relay::send(home,&claim.session,&claim.notice).await,
        "claude_uds"=>claude_send(&claim.session,&claim.notice).await,
        _=>Evidence::new("unsupported_transport",false,false),
    };
    evidence.duration_ms=started.elapsed().as_millis().min(u64::MAX as u128) as u64;complete(home,&claim,&evidence)?;Ok(1)
}
async fn claude_send(session:&str,notice:&Notice)->Evidence{
    let execute=async {
        use std::os::unix::fs::{FileTypeExt,MetadataExt};
        use tokio::{io::AsyncWriteExt,net::UnixStream};
        let host=std::env::var_os("HOME").ok_or_else(||invalid("HOME missing"))?;let registry=std::path::PathBuf::from(host).join(".claude/sessions");let dir=fs::Dir::open(&registry)?;
        let mut entries=std::fs::read_dir(&registry)?.take(513).collect::<std::io::Result<Vec<_>>>()?;if entries.len()>512{return Err(invalid("registry bound exceeded"));}entries.sort_by_key(|e|e.file_name());
        let mut found=None;
        for entry in &entries{let name=entry.file_name();if !name.to_string_lossy().ends_with(".json"){continue;}let Ok(raw)=dir.read(Path::new(&name),64*1024)else{continue;};let Ok(v)=serde_json::from_slice::<Value>(&raw)else{continue;};
            if v["sessionId"].as_str()==Some(session){if let(Some(pid),Some(path))=(v["pid"].as_i64(),v["messagingSocketPath"].as_str()){if pid>1{found=Some((pid,std::path::PathBuf::from(path)));break;}}}}
        let(pid,path)=found.ok_or_else(||invalid("session gone"))?;
        let meta=std::fs::symlink_metadata(&path)?;
        // SAFETY: geteuid has no preconditions.
        if !meta.file_type().is_socket()||meta.uid()!=unsafe{libc::geteuid()}{return Err(invalid("untrusted session socket"));}
        let mut token=None;
        for entry in &entries{let name=entry.file_name();let label=name.to_string_lossy();if !label.starts_with(&format!("{pid}."))||!label.ends_with(".key"){continue;}
            let file=dir.open_file(Path::new(&name))?;let meta=file.metadata()?;if meta.mode()&0o077!=0{return Err(invalid("unsafe inbox key permissions"));}
            let raw=dir.read(Path::new(&name),8192)?;let v:Value=serde_json::from_slice(&raw)?;token=v["peerToken"].as_str().filter(|s|!s.is_empty()).map(str::to_owned);if token.is_some(){break;}}
        let token=token.ok_or_else(||invalid("inbox key missing"))?;
        let mut stream=tokio::time::timeout(Duration::from_secs(1),UnixStream::connect(path)).await.map_err(|_|invalid("session connect timeout"))??;
        let text=notice.render()?;if text.len()>4096{return Err(invalid("notice too large"));}
        let data=format!("{}\n{}\n",json!({"type":"auth","token":token}),json!({"type":"user","message":{"role":"user","content":text}}));
        match tokio::time::timeout(Duration::from_secs(5),stream.write_all(data.as_bytes())).await{Ok(Ok(()))=>Ok::<Evidence,crate::Error>(Evidence::new("uds_written",true,false)),_=>Ok::<Evidence,crate::Error>(Evidence::new("uds_ambiguous",false,true))}
    };
    match execute.await{Ok(evidence)=>evidence,Err(_)=>Evidence::new("uds_unavailable",false,false)}
}
