use crate::{
    redact::{DiagnosticTail, Redactor},
    LaunchPlan,
};
use agent_run_domain::{Error, Result};
use agent_run_platform::{frame, process::OwnedProcess};
use serde_json::{json, Value};
use std::{
    collections::VecDeque,
    process::Stdio,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, Command},
    sync::mpsc,
    task::JoinHandle,
};
pub const ENGINE_FRAME: usize = 8 * 1024 * 1024;
#[derive(Debug)]
pub enum Event {
    Json(Value),
    Eof,
    Failure(&'static str),
}
pub struct Process {
    pub child: Child,
    pub input: Option<ChildStdin>,
    pub owner: OwnedProcess,
    events: mpsc::Receiver<Event>,
    tasks: Vec<JoinHandle<()>>,
    pub backlog: VecDeque<Event>,
    next_id: u64,
    pub stderr_bytes: Arc<AtomicU64>,
    redactor: Redactor,
    diagnostic_tail: Arc<Mutex<DiagnosticTail>>,
}
impl Process {
    pub fn spawn(plan: &LaunchPlan) -> Result<Self> {
        let redactor = Redactor::from_environment(&plan.environment);
        let mut command = Command::new(&plan.binary);
        command
            .args(&plan.args)
            .current_dir(&plan.cwd)
            .env_clear()
            .envs(&plan.environment)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(false);
        use std::os::unix::process::CommandExt;
        command.as_std_mut().process_group(0);
        let mut child = command.spawn()?;
        let pid = child
            .id()
            .ok_or_else(|| Error::Runtime("child PID unavailable".into()))?
            as i32;
        let owner = OwnedProcess::capture(pid);
        let input = child.stdin.take();
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| Error::Runtime("child stdout unavailable".into()))?;
        let mut stderr = child
            .stderr
            .take()
            .ok_or_else(|| Error::Runtime("child stderr unavailable".into()))?;
        let (send, events) = mpsc::channel(8);
        let reader = tokio::spawn(async move {
            let mut reader = BufReader::new(stdout);
            loop {
                let event = match frame::read(&mut reader, ENGINE_FRAME).await {
                    Ok(Some(data)) => {
                        if data.iter().all(u8::is_ascii_whitespace) {
                            continue;
                        }
                        match serde_json::from_slice(&data) {
                            Ok(v) => Event::Json(v),
                            Err(_) => Event::Failure("malformed_engine_json"),
                        }
                    }
                    Ok(None) => Event::Eof,
                    Err(_) => Event::Failure("engine_transport_failure"),
                };
                let done = matches!(event, Event::Eof | Event::Failure(_));
                if send.send(event).await.is_err() || done {
                    break;
                }
            }
        });
        let stderr_bytes = Arc::new(AtomicU64::new(0));
        let count = stderr_bytes.clone();
        let diagnostic_tail = Arc::new(Mutex::new(DiagnosticTail::new(redactor.clone())));
        let tail = diagnostic_tail.clone();
        let errors = tokio::spawn(async move {
            let mut bytes = [0u8; 8192];
            while let Ok(n) = stderr.read(&mut bytes).await {
                if n == 0 {
                    break;
                }
                count.fetch_add(n as u64, Ordering::Relaxed);
                if let Ok(mut tail) = tail.lock() {
                    tail.push(&bytes[..n]);
                }
            }
        });
        Ok(Self {
            child,
            input,
            owner,
            events,
            tasks: vec![reader, errors],
            backlog: VecDeque::new(),
            next_id: 1,
            stderr_bytes,
            redactor,
            diagnostic_tail,
        })
    }

    /// Redacts launch secrets from text before core code persists it.
    pub fn redact(&self, text: &str) -> String {
        self.redactor.redact(text)
    }

    /// Returns the redacted bounded stderr evidence retained for a failed launch.
    pub fn diagnostic_tail(&self) -> Option<String> {
        self.diagnostic_tail
            .lock()
            .ok()
            .and_then(|tail| tail.text())
    }
    pub async fn send(&mut self, message: &Value) -> Result<()> {
        let input = self
            .input
            .as_mut()
            .ok_or_else(|| Error::Runtime("engine stdin is closed".into()))?;
        tokio::time::timeout(
            Duration::from_secs(15),
            frame::write(input, message, ENGINE_FRAME),
        )
        .await
        .map_err(|_| Error::Runtime("engine input delivery timed out".into()))?
    }
    pub async fn text(&mut self, text: &str) -> Result<()> {
        let input = self
            .input
            .as_mut()
            .ok_or_else(|| Error::Runtime("engine stdin is closed".into()))?;
        tokio::time::timeout(Duration::from_secs(15), input.write_all(text.as_bytes()))
            .await
            .map_err(|_| Error::Runtime("engine input delivery timed out".into()))??;
        input.flush().await?;
        Ok(())
    }
    pub async fn next(&mut self) -> Event {
        if let Some(e) = self.backlog.pop_front() {
            e
        } else {
            self.events.recv().await.unwrap_or(Event::Eof)
        }
    }
    pub async fn rpc(&mut self, method: &str, params: Value, timeout: Duration) -> Result<Value> {
        let id = self.next_id;
        self.next_id += 1;
        self.send(&json!({"id":id,"method":method,"params":params}))
            .await?;
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let event = tokio::time::timeout_at(deadline, self.events.recv())
                .await
                .map_err(|_| Error::Runtime("app-server RPC timed out".into()))?
                .unwrap_or(Event::Eof);
            match event {
                Event::Json(v)
                    if v.get("id").and_then(Value::as_u64) == Some(id)
                        && v.get("method").is_none() =>
                {
                    if v.get("error").is_some() {
                        return Err(Error::Runtime("app-server rejected RPC request".into()));
                    }
                    return v
                        .get("result")
                        .cloned()
                        .ok_or_else(|| Error::Runtime("RPC result is missing".into()));
                }
                Event::Json(v) if v.get("method").is_some() && v.get("id").is_some() => {
                    self.deny_request(&v).await?;
                }
                Event::Json(v) => {
                    if self.backlog.len() >= 16 {
                        return Err(Error::Runtime("app-server startup event overflow".into()));
                    }
                    self.backlog.push_back(Event::Json(v));
                }
                Event::Eof => return Err(Error::Runtime("app-server EOF".into())),
                Event::Failure(kind) => return Err(Error::Runtime(kind.into())),
            }
        }
    }
    pub async fn deny_request(&mut self, v: &Value) -> Result<()> {
        let method = v.get("method").and_then(Value::as_str).unwrap_or("");
        let response = if method == "item/permissions/requestApproval" {
            json!({"id":v["id"],"result":{"permissions":{},"scope":"turn"}})
        } else if method.ends_with("requestApproval") {
            json!({"id":v["id"],"result":{"decision":"decline"}})
        } else {
            json!({"id":v["id"],"error":{"code":-32601,"message":"interactive request unavailable in durable headless execution"}})
        };
        self.send(&response).await
    }
    pub async fn reap(&mut self) -> Option<i32> {
        let code = match tokio::time::timeout(Duration::from_secs(3), self.child.wait()).await {
            Ok(Ok(s)) => s.code(),
            _ => None,
        };
        for task in self.tasks.drain(..) {
            let _ = task.await;
        }
        code
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        for t in &self.tasks {
            t.abort();
        }
    }
}
