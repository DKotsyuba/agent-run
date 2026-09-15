//! Bounded LF-delimited JSON-RPC 2.0 over a private same-user Unix socket.
use super::frame;
use crate::{dispatch, fs, service::Service, Error, Result};
use fs2::FileExt;
use serde_json::{json, Value};
use std::{
    fs::{File, OpenOptions},
    os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::BufReader,
    net::{UnixListener, UnixStream},
    sync::Semaphore,
};
pub const MAX_FRAME: usize = 1024 * 1024;
struct SocketGuard {
    path: PathBuf,
    device: u64,
    inode: u64,
    _lock: File,
}
impl Drop for SocketGuard {
    fn drop(&mut self) {
        if let Ok(m) = std::fs::symlink_metadata(&self.path) {
            if m.dev() == self.device && m.ino() == self.inode && m.file_type().is_socket() {
                let _ = std::fs::remove_file(&self.path);
            }
        }
    }
}
fn err(id: Value, code: i32, message: &str, data: Option<Value>) -> Value {
    json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message,"data":data}})
}
pub async fn respond(service: &Service, v: Value) -> Option<Value> {
    let Some(obj) = v.as_object() else {
        return Some(err(Value::Null, -32600, "request must be an object", None));
    };
    let id = obj.get("id").cloned().unwrap_or(Value::Null);
    let valid_id = id.is_null() || id.is_string() || id.is_number();
    if !valid_id
        || obj.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        || !obj.get("method").is_some_and(Value::is_string)
    {
        return Some(err(Value::Null, -32600, "invalid JSON-RPC request", None));
    }
    let method = obj.get("method").and_then(Value::as_str).unwrap_or("");
    let notification = !obj.contains_key("id");
    let known = dispatch::is_tool(method) || ["tools", "ping", "wait"].contains(&method);
    let response = if !known {
        err(id, -32601, "method not found", None)
    } else {
        match dispatch::call(
            service,
            method,
            obj.get("params").cloned().unwrap_or_else(|| json!({})),
        )
        .await
        {
            Ok(value) => json!({"jsonrpc":"2.0","id":id,"result":value}),
            Err(e) => {
                let public = e.public();
                err(
                    id,
                    e.rpc_code(),
                    &public.message,
                    Some(json!({"kind":public.kind})),
                )
            }
        }
    };
    if notification {
        None
    } else {
        Some(response)
    }
}
async fn connection(stream: UnixStream, service: Service) -> Result<()> {
    // SAFETY: geteuid has no arguments or memory safety preconditions.
    let uid = unsafe { libc::geteuid() };
    if stream.peer_cred()?.uid() != uid {
        return Err(crate::error::invalid("socket peer has another user ID"));
    }
    let (input, mut output) = stream.into_split();
    let mut input = BufReader::new(input);
    loop {
        let raw = match tokio::time::timeout(
            Duration::from_secs(120),
            frame::read(&mut input, MAX_FRAME),
        )
        .await
        {
            Ok(Ok(Some(raw))) => raw,
            Ok(Ok(None)) => return Ok(()),
            _ => {
                return Err(crate::error::invalid(
                    "socket input ended or exceeded its bound",
                ))
            }
        };
        let response = match serde_json::from_slice(&raw) {
            Ok(value) => respond(&service, value).await,
            Err(_) => Some(err(Value::Null, -32700, "invalid JSON", None)),
        };
        if let Some(mut response) = response {
            if serde_json::to_vec(&response)?.len() + 1 > MAX_FRAME {
                response = err(
                    response["id"].clone(),
                    -32603,
                    "response exceeds maximum size",
                    None,
                );
            }
            tokio::time::timeout(
                Duration::from_secs(15),
                frame::write(&mut output, &response, MAX_FRAME),
            )
            .await
            .map_err(|_| crate::error::invalid("socket output timeout"))??;
        }
    }
}
pub async fn serve(home: &Path) -> Result<()> {
    let _ = crate::config::Config::load(home)?;
    let _ = crate::state::Store::open(home)?;
    let directory = std::fs::symlink_metadata(home)?;
    // SAFETY: geteuid is a read-only process identity query.
    let uid = unsafe { libc::geteuid() };
    if !directory.is_dir()
        || directory.file_type().is_symlink()
        || directory.uid() != uid
        || directory.mode() & 0o077 != 0
    {
        return Err(crate::error::invalid(
            "agent-run home must be an owned mode-0700 directory",
        ));
    }
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(home.join("api.lock"))?;
    lock.try_lock_exclusive()
        .map_err(|_| crate::error::invalid("another broker holds api.lock"))?;
    let path = home.join("api.sock");
    if let Ok(meta) = std::fs::symlink_metadata(&path) {
        if !meta.file_type().is_socket() || meta.uid() != uid {
            return Err(crate::error::invalid(
                "refusing to replace non-socket or foreign socket path",
            ));
        }
        match UnixStream::connect(&path).await {
            Ok(_) => return Err(crate::error::invalid("another broker is listening")),
            Err(e)
                if [Some(libc::ECONNREFUSED), Some(libc::ENOENT)].contains(&e.raw_os_error()) =>
            {
                std::fs::remove_file(&path)?
            }
            Err(e) => return Err(e.into()),
        }
    }
    let listener = UnixListener::bind(&path)?;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    let meta = std::fs::symlink_metadata(&path)?;
    let _guard = SocketGuard {
        path,
        device: meta.dev(),
        inode: meta.ino(),
        _lock: lock,
    };
    let service = Service::new(home.to_owned());
    let _ = service.reconcile()?;
    let maintenance = service.clone();
    let worker = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            let _ = maintenance.reconcile();
            if let Err(e) = crate::delivery::dispatch_once(&maintenance.home).await {
                eprintln!("delivery maintenance: {}", e.public().kind);
            }
        }
    });
    let gate = Arc::new(Semaphore::new(128));
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    loop {
        tokio::select! {
            _=tokio::signal::ctrl_c()=>break,
            _=term.recv()=>break,
            accepted=listener.accept()=>{
                let(stream,_)=accepted?;let Ok(permit)=gate.clone().try_acquire_owned()else{drop(stream);continue;};let service=service.clone();
                tokio::spawn(async move{let _permit=permit;let _=connection(stream,service).await;});
            }
        }
    }
    worker.abort();
    Ok(())
}
/// A separate connection for every call; no shared response-routing state.
pub async fn client(home: &Path, method: &str, params: Value) -> Result<Value> {
    let mut stream = tokio::time::timeout(
        Duration::from_secs(3),
        UnixStream::connect(home.join("api.sock")),
    )
    .await
    .map_err(|_| Error::BrokerUnavailable)?
    .map_err(|_| Error::BrokerUnavailable)?;
    // SAFETY: geteuid is a read-only query.
    if stream.peer_cred()?.uid() != unsafe { libc::geteuid() } {
        return Err(crate::error::invalid("broker belongs to another user"));
    }
    let id = uuid::Uuid::new_v4().to_string();
    tokio::time::timeout(
        Duration::from_secs(15),
        frame::write(
            &mut stream,
            &json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}),
            MAX_FRAME,
        ),
    )
    .await
    .map_err(|_| {
        Error::Runtime("broker request write deadline exceeded; admission may be ambiguous".into())
    })??;
    let mut input = BufReader::new(stream);
    // wait deliberately has no client-owned execution deadline.
    let raw = if method == "wait" {
        frame::read(&mut input, MAX_FRAME).await?
    } else {
        tokio::time::timeout(Duration::from_secs(120), frame::read(&mut input, MAX_FRAME))
            .await
            .map_err(|_| {
                Error::Runtime(
                    "broker response deadline exceeded; an admitted run is not cancelled".into(),
                )
            })??
    };
    let value: Value = serde_json::from_slice(
        &raw.ok_or_else(|| Error::Runtime("broker disconnected before its response".into()))?,
    )?;
    if value.get("id").and_then(Value::as_str) != Some(id.as_str())
        || value.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
    {
        return Err(crate::error::invalid("invalid broker response identity"));
    }
    if let Some(error) = value.get("error") {
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("broker request failed")
            .chars()
            .take(512)
            .collect();
        return Err(
            if error.get("code").and_then(Value::as_i64) == Some(-32602) {
                Error::Validation(message)
            } else {
                Error::Runtime(message)
            },
        );
    }
    value
        .get("result")
        .cloned()
        .ok_or_else(|| crate::error::invalid("broker returned neither result nor error"))
}
