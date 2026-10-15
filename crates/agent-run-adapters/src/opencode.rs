//! Owned OpenCode v2 configuration and bounded loopback HTTP transport.
use agent_run_config::{
    config::{Config, Runtime},
    profiles::Profile,
};
use agent_run_domain::{Result, error::invalid};
use serde_json::{Value, json};
use std::{collections::BTreeSet, path::Path, time::Duration};
use url::Url;

/// Native managed-agent identity; unrelated stock agents are never selected.
pub const AGENT: &str = "agent-run";
/// Maximum bytes accepted from one native HTTP response or SSE frame.
const MAX_FRAME: usize = 8 * 1024 * 1024;

/// Rejects unqualified native features instead of silently weakening a role.
pub fn validate(runtime: &Runtime, profile: &Profile) -> Result<()> {
    if !runtime.hooks.is_empty()
        || !runtime.plugins.is_empty()
        || !runtime.native_settings.is_empty()
        || !runtime.workspace_roots.is_empty()
        || runtime.workspace_network
    {
        return Err(invalid(
            "OpenCode hooks, plugins, workspace roots and native tuning are not supported",
        ));
    }
    let prefixes: BTreeSet<_> = profile
        .mcp
        .iter()
        .map(|server| tool_prefix(server))
        .collect();
    if prefixes.len() != profile.mcp.len()
        || prefixes.iter().any(|prefix| {
            prefix == "opencode"
                || prefix.starts_with("opencode_")
                || prefix
                    .char_indices()
                    .any(|(index, c)| c == '_' && prefixes.contains(&prefix[..index]))
        })
    {
        return Err(invalid(
            "OpenCode MCP namespaces overlap or use a reserved native namespace",
        ));
    }
    if profile.research_tools_only() {
        return Err(invalid("OpenCode restricted research is not qualified"));
    }
    Ok(())
}

/// Splits an explicit native provider/model reference; no default provider is guessed.
pub fn model_ref(model: &str) -> Result<Value> {
    let (provider, id) = model
        .split_once('/')
        .ok_or_else(|| invalid("OpenCode native_model must be provider/model"))?;
    if [provider, id].iter().any(|part| {
        part.is_empty()
            || !part
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
    }) {
        return Err(invalid("invalid OpenCode provider/model"));
    }
    Ok(json!({"providerID":provider,"id":id}))
}

/// Escapes one opaque query value without allowing it to choose an API endpoint.
pub fn query_value(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

/// Converts an admitted server name to the native MCP permission namespace.
pub fn tool_prefix(server: &str) -> String {
    server
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Builds ordered native tool permissions from a validated frozen role.
/// `home` names its sealed skills; `workdir` is the sole native editor scope.
/// Unknown filtered MCP names refuse. Skills are readable and editor-denied;
/// these filters do not claim an OS filesystem or network sandbox.
pub fn permissions(profile: &Profile, home: &Path, workdir: &Path) -> Result<Value> {
    let mut permissions = vec![
        json!({"action":"*","resource":"*","effect":"deny"}),
        // OpenCode free tier requires its native shell tool to remain visible.
        // Read-only execution is blocked by the runner rejecting every ask.
        json!({"action":"shell","resource":"*","effect":"ask"}),
    ];
    for action in ["read", "glob", "grep"] {
        permissions.push(json!({"action":action,"resource":"*","effect":"allow"}));
    }
    for skill in &profile.skills {
        permissions.push(json!({"action":"skill","resource":skill,"effect":"allow"}));
    }
    if !profile.skills.is_empty() {
        permissions.push(json!({"action":"external_directory","resource":format!("{}/skills/**",home.display()),"effect":"allow"}));
    }
    if profile.write {
        for action in ["edit", "shell"] {
            permissions.push(json!({"action":action,"resource":if action=="edit" {format!("{}/**",workdir.display())} else {"*".into()},"effect":"allow"}));
        }
    }
    if profile.network {
        for action in ["webfetch", "websearch"] {
            permissions.push(json!({"action":action,"resource":"*","effect":"allow"}));
        }
    }
    for root in &profile.read_roots {
        permissions.push(json!({"action":"external_directory","resource":format!("{}/*",root.display()),"effect":"allow"}));
    }
    for name in &profile.mcp {
        let prefix = tool_prefix(name);
        let allowed = profile.mcp_tools.get(name);
        if let Some(tools) = allowed {
            for tool in tools {
                if tool.is_empty() || tool_prefix(tool) != *tool {
                    return Err(invalid(
                        "OpenCode filtered MCP tool names must be literal ASCII identifiers",
                    ));
                }

                permissions.push(json!({"action":format!("{prefix}_{}",tool_prefix(tool)),"resource":"*","effect":"allow"}));
            }
        } else {
            permissions
                .push(json!({"action":format!("{prefix}_*"),"resource":"*","effect":"allow"}));
        }
    }
    if !profile.skills.is_empty() {
        permissions.push(json!({"action":"edit","resource":format!("{}/skills/**",home.display()),"effect":"deny"}));
    }
    for action in ["read", "edit"] {
        for pattern in [".env", ".env.*", "*/.env", "*/.env.*", "*.pem", "*.key"] {
            permissions.push(json!({"action":action,"resource":pattern,"effect":"deny"}));
        }
    }
    Ok(json!(permissions))
}

/// Renders only frozen skills, native permissions and selected local MCPs.
/// `home` stores sealed assets and `workdir` grants editor access; no I/O or
/// credential lookup occurs. Missing servers/unsupported role features refuse.
pub fn render(
    config: &Config,
    runtime: &Runtime,
    profile: &Profile,
    home: &Path,
    workdir: &Path,
) -> Result<Value> {
    validate(runtime, profile)?;
    let permissions = permissions(profile, home, workdir)?;
    let mut servers = serde_json::Map::new();
    for name in &profile.mcp {
        let server = config
            .mcp
            .get(name)
            .ok_or_else(|| invalid("OpenCode selected MCP is unavailable"))?;
        let mut command = vec![server.command.to_string_lossy().into_owned()];
        command.extend(server.args.clone());
        // Inherit the already filtered child's environment; capability values never enter this file.
        servers.insert(
            name.clone(),
            json!({"type":"local","command":command,"codemode":false}),
        );
    }
    Ok(json!({
        "agents":{AGENT:{"system":profile.body,"mode":"primary","permissions":permissions},"title":{"disabled":true},"summary":{"disabled":true}},
        "permissions":[{"action":"*","resource":"*","effect":"deny"}],
        "mcp":{"servers":servers},
        "skills":profile.skills.iter().map(|name|home.join("skills").join(name).to_string_lossy().into_owned()).collect::<Vec<_>>(),
        "plugins":[]
    }))
}

/// Authenticated client for one verified owned native server. Secrets are not Debug/Serialize.
pub struct Client {
    /// Private loopback origin learned from the owned child's readiness envelope.
    origin: Url,
    /// Redirect/proxy-free HTTP transport, never an inference client.
    http: reqwest::Client,
    /// Attempt-local server lease password, distinct from the worker capability.
    password: String,
}
impl Client {
    /// Requires an explicit loopback HTTP origin with no credentials or extra components.
    pub fn new(value: &str, password: String) -> Result<Self> {
        let origin = Url::parse(value).map_err(|_| invalid("invalid OpenCode readiness URL"))?;
        if origin.scheme() != "http"
            || origin.host_str() != Some("127.0.0.1")
            || origin.port().is_none_or(|port| port == 0)
            || origin.path() != "/"
            || origin.query().is_some()
            || origin.fragment().is_some()
            || !origin.username().is_empty()
            || origin.password().is_some()
            || password.is_empty()
        {
            return Err(invalid(
                "OpenCode readiness is not an owned loopback origin",
            ));
        }
        let http = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(5))
            .build()
            .map_err(|_| invalid("OpenCode HTTP client could not be constructed"))?;
        Ok(Self {
            origin,
            http,
            password,
        })
    }
    /// Builds one credential-bearing request to this origin only.
    fn request(&self, method: reqwest::Method, route: &str) -> Result<reqwest::RequestBuilder> {
        if !route.starts_with("/api/") || route.starts_with("//") || route.contains(['#', '\\']) {
            return Err(invalid("invalid OpenCode API route"));
        }
        let url = self
            .origin
            .join(route)
            .map_err(|_| invalid("invalid OpenCode API route"))?;
        if url.origin() != self.origin.origin() || !url.path().starts_with("/api/") {
            return Err(invalid("OpenCode API escaped its origin"));
        }
        Ok(self
            .http
            .request(method, url)
            .basic_auth("opencode", Some(&self.password)))
    }
    /// Sends one finite control request without retries; non-success stays a typed refusal.
    pub async fn call(
        &self,
        method: reqwest::Method,
        route: &str,
        body: Option<Value>,
    ) -> Result<Value> {
        self.call_for(method, route, body, Duration::from_secs(30))
            .await
    }

    /// Sends a finite control under its caller-owned allowance, without any native replay.
    pub async fn call_for(
        &self,
        method: reqwest::Method,
        route: &str,
        body: Option<Value>,
        allowance: Duration,
    ) -> Result<Value> {
        let allowance = if route.ends_with("/interrupt") {
            allowance.min(Duration::from_secs(1))
        } else {
            allowance
        };
        self.call_until(method, route, body, tokio::time::Instant::now() + allowance)
            .await
    }

    /// Sends one control under the original absolute deadline, including body reads.
    /// Expired calls fail before opening a connection; transport/ACK loss is never replayed.
    pub async fn call_until(
        &self,
        method: reqwest::Method,
        route: &str,
        body: Option<Value>,
        deadline: tokio::time::Instant,
    ) -> Result<Value> {
        let mut request = self.request(method, route)?;
        if let Some(body) = body {
            request = request
                .header("content-type", "application/json")
                .body(serde_json::to_vec(&body)?);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(invalid("OpenCode control deadline elapsed"));
        }
        let mut response = request
            .timeout(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .send()
            .await
            .map_err(|_| invalid("OpenCode control transport failed"))?;
        if !response.status().is_success() {
            return Err(invalid(format!(
                "OpenCode control rejected (HTTP {})",
                response.status().as_u16()
            )));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| invalid("OpenCode response transport failed"))?
        {
            if bytes.len().saturating_add(chunk.len()) > MAX_FRAME {
                return Err(invalid("OpenCode response exceeds bound"));
            }
            bytes.extend_from_slice(&chunk);
        }
        if bytes.is_empty() {
            return Ok(Value::Null);
        }
        Ok(serde_json::from_slice(&bytes)?)
    }
    /// Opens the volatile native event stream; its body has no lifetime deadline.
    /// Standard v2.0.28 serve does not persist event payloads, so recovery uses messages.
    pub async fn subscribe(&self) -> Result<Log> {
        let route = "/api/event";
        let response = tokio::time::timeout(
            Duration::from_secs(10),
            self.request(reqwest::Method::GET, route)?.send(),
        )
        .await
        .map_err(|_| invalid("OpenCode log connection timed out"))?
        .map_err(|_| invalid("OpenCode log connection failed"))?;
        if !response.status().is_success()
            || !response
                .headers()
                .get("content-type")
                .is_some_and(|v| v.as_bytes().starts_with(b"text/event-stream"))
        {
            return Err(invalid("OpenCode log is unavailable"));
        }
        Ok(Log {
            response,
            pending: Vec::new(),
            line: Vec::new(),
            data: String::new(),
            cr: false,
        })
    }
}

/// Bounded SSE reader for native JSON events; no retry policy or hidden buffering task.
pub struct Log {
    /// Long-lived response owned by this session only.
    response: reqwest::Response,
    /// Unconsumed bytes of the current HTTP chunk.
    pending: Vec<u8>,
    /// One UTF-8 SSE line, retained across arbitrary chunk boundaries.
    line: Vec<u8>,
    /// Joined data lines for the current frame.
    data: String,
    /// Whether the previous byte ended a line with CR (so its LF is ignored).
    cr: bool,
}
impl Log {
    /// Returns one JSON data frame, preserving UTF-8, CR/LF and multiline SSE framing.
    /// EOF with partial data is a refusal rather than a fabricated final event.
    pub async fn next(&mut self) -> Result<Option<Value>> {
        loop {
            if self.pending.is_empty() {
                match self
                    .response
                    .chunk()
                    .await
                    .map_err(|_| invalid("OpenCode log transport failed"))?
                {
                    Some(chunk) if chunk.len() <= MAX_FRAME => {
                        self.pending.extend_from_slice(&chunk)
                    }
                    Some(_) => return Err(invalid("OpenCode HTTP chunk exceeds bound")),
                    None if self.line.is_empty() && self.data.is_empty() => return Ok(None),
                    None => return Err(invalid("OpenCode log ended in a partial frame")),
                }
            }
            let bytes = std::mem::take(&mut self.pending);
            for (index, byte) in bytes.iter().copied().enumerate() {
                if self.cr {
                    self.cr = false;
                    if byte == b'\n' {
                        continue;
                    }
                }
                if byte == b'\r' || byte == b'\n' {
                    self.cr = byte == b'\r';
                    let line = std::str::from_utf8(&self.line)
                        .map_err(|_| invalid("OpenCode SSE line is not UTF-8"))?;
                    if line.is_empty() && !self.data.is_empty() {
                        let event = serde_json::from_str(self.data.trim_end_matches('\n'))?;
                        self.data.clear();
                        self.line.clear();
                        self.pending.extend_from_slice(&bytes[index + 1..]);
                        return Ok(Some(event));
                    }
                    if let Some(value) = line.strip_prefix("data:") {
                        self.data.push_str(value.strip_prefix(' ').unwrap_or(value));
                        self.data.push('\n');
                    } else if line == "data" {
                        self.data.push('\n');
                    }
                    self.line.clear();
                } else {
                    self.line.push(byte);
                }
                if self.line.len().saturating_add(self.data.len()) > MAX_FRAME {
                    return Err(invalid("OpenCode SSE frame exceeds bound"));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    /// Readiness may never redirect credentials to a non-owned listener.
    #[test]
    fn readiness_requires_plain_explicit_loopback() {
        for url in [
            "https://127.0.0.1:1",
            "http://localhost:1",
            "http://127.0.0.1",
            "http://127.0.0.1:1/path",
            "http://127.0.0.1:1?x=1",
            "http://user@127.0.0.1:1",
            "http://example.com:1",
        ] {
            assert!(Client::new(url, "secret".into()).is_err(), "{url}");
        }
        assert!(Client::new("http://127.0.0.1:12345", "secret".into()).is_ok());
    }
    /// A native model reference cannot carry paths, query strings or default aliases.
    #[test]
    fn model_reference_is_explicit() {
        assert_eq!(
            model_ref("opencode/big-pickle").unwrap(),
            json!({"providerID":"opencode","id":"big-pickle"})
        );
        for value in [
            "big-pickle",
            "opencode/",
            "/big-pickle",
            "opencode/x/y",
            "opencode/x?key=y",
        ] {
            assert!(model_ref(value).is_err());
        }
    }
    /// Serves exactly one raw response and verifies the private credential transport.
    /// No model, external network or background service participates in these checks.
    async fn response(bytes: Vec<u8>) -> (Client, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = BufReader::new(stream);
            let mut headers = String::new();
            loop {
                let mut line = String::new();
                assert!(stream.read_line(&mut line).await.unwrap() > 0);
                if line == "\r\n" {
                    break;
                }
                headers.push_str(&line);
            }
            assert!(
                headers
                    .to_ascii_lowercase()
                    .contains("authorization: basic b3blbmnvzgu6c2vjcmv0\r\n")
            );
            stream.get_mut().write_all(&bytes).await.unwrap();
            stream.get_mut().shutdown().await.unwrap();
        });
        (Client::new(&url, "secret".into()).unwrap(), server)
    }

    /// An expired owner budget opens no socket; a response body cannot renew the original deadline.
    #[tokio::test]
    async fn control_deadline_prevents_expired_effects_and_bounds_body_reads() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = Client::new(
            &format!("http://{}", listener.local_addr().unwrap()),
            "probe".into(),
        )
        .unwrap();
        assert!(
            client
                .call_until(
                    reqwest::Method::POST,
                    "/api/session/ses_one/prompt",
                    Some(json!({"text":"must not send"})),
                    tokio::time::Instant::now() - Duration::from_millis(1)
                )
                .await
                .is_err()
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(20), listener.accept())
                .await
                .is_err()
        );
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = BufReader::new(stream);
            loop {
                let mut line = String::new();
                assert!(stream.read_line(&mut line).await.unwrap() > 0);
                if line == "\r\n" {
                    break;
                }
            }
            stream
                .get_mut()
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{")
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(500)).await;
            let _ = stream.get_mut().write_all(b"}").await;
        });
        let result = tokio::time::timeout(
            Duration::from_millis(250),
            client.call_until(
                reqwest::Method::GET,
                "/api/info",
                None,
                tokio::time::Instant::now() + Duration::from_millis(25),
            ),
        )
        .await
        .unwrap();
        assert!(result.is_err());
        server.await.unwrap();
    }

    /// Fragmented UTF-8, comments, all SSE newline forms and multiple frames survive.
    /// A disconnected partial frame never invents a final event.
    #[tokio::test]
    async fn event_stream_preserves_native_framing_and_refuses_truncation() {
        let body =
            ": heartbeat\r\ndata: {\r\ndata: \"type\":\"пример\"}\r\n\r\ndata: {\"n\":2}\r\r"
                .as_bytes();
        let mut wire=b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec();
        for byte in body {
            wire.extend_from_slice(b"1\r\n");
            wire.push(*byte);
            wire.extend_from_slice(b"\r\n");
        }
        wire.extend_from_slice(b"0\r\n\r\n");
        let (client, server) = response(wire).await;
        let mut log = client.subscribe().await.unwrap();
        assert_eq!(log.next().await.unwrap(), Some(json!({"type":"пример"})));
        assert_eq!(log.next().await.unwrap(), Some(json!({"n":2})));
        assert!(log.next().await.unwrap().is_none());
        server.await.unwrap();
        let body = b"data: {\"unfinished\":true}";
        let wire = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\n\r\n",
            body.len()
        )
        .into_bytes()
        .into_iter()
        .chain(body.iter().copied())
        .collect();
        let (client, server) = response(wire).await;
        assert!(client.subscribe().await.unwrap().next().await.is_err());
        server.await.unwrap();
    }

    /// Control requests neither follow redirects nor emit raw response/credential text.
    #[tokio::test]
    async fn control_refuses_redirects_and_foreign_routes() {
        let (client,server)=response(b"HTTP/1.1 302 Found\r\nLocation: http://example.com/secret\r\nContent-Length: 0\r\n\r\n".to_vec()).await;
        let error = client
            .call(reqwest::Method::GET, "/api/info", None)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("302"));
        assert!(!error.contains("secret"));
        for route in [
            "//example.com/api/info",
            "https://example.com/api/info",
            "/api/../private",
            "/api/info#x",
        ] {
            assert!(
                client.request(reqwest::Method::GET, route).is_err(),
                "{route}"
            );
        }
        server.await.unwrap();
        assert_eq!(query_value("a?b&c#d"), "a%3Fb%26c%23d");
    }

    /// Frozen role grants produce ordered native denies and exact MCP permissions;
    /// no tool credential is written into the native configuration.
    #[test]
    fn rendered_role_preserves_read_only_and_exact_tool_caps() {
        let request=serde_json::from_value(json!({"runtime":"opencode","model":"free","profile":"review","task":"canary","workdir":"/workspace"})).unwrap();
        let mut profile=agent_run_config::profiles::parse("+++\nrevision='one'\nwrite=false\nnetwork=false\nallow_external_read_roots=false\nskills=[]\nmcp=[]\nrequired_constraints=[]\n+++\nReview only.",&request).unwrap();
        let runtime:Runtime=serde_json::from_value(json!({"enabled":true,"adapter":"opencode","binary":"/usr/bin/true","home":"/native","models":["free"]})).unwrap();
        let mut config: Config = serde_json::from_value(json!({"schema_version":1})).unwrap();
        profile.mcp = vec!["agent_run_worker".into()];
        profile
            .mcp_tools
            .insert("agent_run_worker".into(), vec!["finish".into()]);
        config.mcp.insert("agent_run_worker".into(),serde_json::from_value(json!({"transport":"stdio","command":"/usr/bin/true","env_from":["PRIVATE_CAPABILITY"]})).unwrap());
        let native = render(
            &config,
            &runtime,
            &profile,
            Path::new("/native"),
            Path::new("/workspace"),
        )
        .unwrap();
        let rules = native["agents"][AGENT]["permissions"].as_array().unwrap();
        assert_eq!(
            rules[0],
            json!({"action":"*","resource":"*","effect":"deny"})
        );
        assert!(rules.contains(&json!({"action":"shell","resource":"*","effect":"ask"})));
        assert!(
            !rules
                .iter()
                .any(|rule| rule["action"] == "edit" && rule["effect"] == "allow")
        );
        assert!(rules.contains(
            &json!({"action":"agent_run_worker_finish","resource":"*","effect":"allow"})
        ));
        assert!(
            !rules
                .iter()
                .any(|rule| rule["action"] == "agent_run_worker_*")
        );
        assert_eq!(
            native["mcp"]["servers"]["agent_run_worker"]["codemode"],
            false
        );
        assert!(!native.to_string().contains("PRIVATE_CAPABILITY"));
        profile.mcp.push("agent.run.worker".into());
        assert!(
            render(
                &config,
                &runtime,
                &profile,
                Path::new("/native"),
                Path::new("/workspace")
            )
            .is_err()
        );
    }
}
