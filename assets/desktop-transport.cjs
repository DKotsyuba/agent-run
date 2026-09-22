/** Signed-Node MCP frontend: fixed completion notices, never arbitrary RPC. */
"use strict";
const crypto = require("node:crypto");
const fs = require("node:fs");
const net = require("node:net");
const path = require("node:path");
const { spawn } = require("node:child_process");

/** Frame limits, host deadline, and local concurrency ceiling. */
const LOCAL_LIMIT = 8192, HOST_LIMIT = 8 * 1024 * 1024, HOST_MS = 8000, MAX_CONNECTIONS = 8;
/** Rust executable, private home, embedded notice contract, and exact original MCP arguments. */
const [executable, home, contractJson, ...mcpArgs] = process.argv.slice(1);
if (!path.isAbsolute(executable || "") || !path.isAbsolute(home || "") || !contractJson)
  throw new Error("invalid frontend arguments");
/** Desktop native-tools pipe inherited only by this signed frontend. */
const pipe = process.env.CODEX_APP_TOOLS_PIPE_PATH;
/** Private relay endpoint with a random suffix so PID reuse cannot collide. */
const relayPath = path.join(home, `ar-cdx-v3-${process.pid}-${crypto.randomBytes(3).toString("hex")}.sock`);
/** Trusted completion template embedded by the Rust executable. */
const NOTICE_CONTRACT = JSON.parse(contractJson);
/** Notice marker version, independent of the accepted relay wire versions. */
const COMPLETION_NOTICE_VERSION = 1;
/** Exact request shapes for legacy v1, selector-rich v2, and failure-aware v3. */
const LEGACY_KEYS = ["agent_id", "notification_id", "op", "status", "thread_id", "version"];
const V2_KEYS = ["agent_id", "effort", "model", "notification_id", "op", "runtime", "status", "thread_id", "version"];
const V3_KEYS = ["agent_id", "effort", "failure_kind", "model", "notification_id", "op", "runtime", "status", "thread_id", "version"];
/** Launch metadata bound in code points, matching the Rust notice contract. */
const META_LIMIT = 128;

/** Encode a bounded JSON object as a uint32-LE frame. */
function frame(value, limit) {
  const body = Buffer.from(JSON.stringify(value));
  if (!body.length || body.length > limit) throw new Error("frame limit");
  const header = Buffer.alloc(4);
  header.writeUInt32LE(body.length);
  return Buffer.concat([header, body]);
}

/** Read one frame before deadline and remove every listener when settled. */
function receive(socket, limit, deadline) {
  return new Promise((resolve, reject) => {
    let data = Buffer.alloc(0), size;
    const finish = (error, value) => {
      clearTimeout(timer);
      socket.off("data", onData); socket.off("error", onError); socket.off("close", onClose);
      error ? reject(error) : resolve(value);
    };
    const onError = error => finish(error);
    const onClose = () => finish(new Error("closed"));
    const onData = chunk => {
      if (data.length + chunk.length > limit + 4) return finish(new Error("frame limit"));
      data = Buffer.concat([data, chunk]);
      if (size === undefined && data.length >= 4) {
        size = data.readUInt32LE(0);
        if (!size || size > limit) return finish(new Error("frame limit"));
      }
      if (size !== undefined && data.length >= size + 4) {
        try { finish(null, JSON.parse(data.subarray(4, size + 4).toString("utf8"))); }
        catch (error) { finish(error); }
      }
    };
    const timer = setTimeout(() => finish(new Error("timeout")), Math.max(1, deadline - Date.now()));
    socket.on("data", onData); socket.once("error", onError); socket.once("close", onClose);
  });
}

/** Connect before the shared deadline; no completion has been sent yet. */
function connect(socket, endpoint, deadline) {
  return new Promise((resolve, reject) => {
    const finish = error => {
      clearTimeout(timer); socket.off("connect", onConnect); socket.off("error", onError);
      error ? reject(error) : resolve();
    };
    const onConnect = () => finish();
    const onError = error => finish(error);
    const timer = setTimeout(() => finish(new Error("timeout")), Math.max(1, deadline - Date.now()));
    socket.once("connect", onConnect); socket.once("error", onError); socket.connect(endpoint);
  });
}

/** Exchange one correlated host request; any malformed envelope is a failure. */
async function rpc(socket, id, method, params, deadline) {
  const reply = receive(socket, HOST_LIMIT, deadline);
  socket.write(frame({ jsonrpc: "2.0", id, method, params }, HOST_LIMIT));
  const value = await reply;
  if (!value || value.jsonrpc !== "2.0" || value.id !== id || "error" in value || !value.result)
    throw new Error("invalid response");
  return value.result;
}

/** Escape controls and Unicode line separators as literal uXXXX sequences, matching Rust. */
function escapeMeta(value) {
  let out = "";
  for (const ch of value) {
    const code = ch.codePointAt(0);
    out += code < 0x20 || (code >= 0x7f && code <= 0x9f) || code === 0x2028 || code === 0x2029
      ? "\\u" + code.toString(16).padStart(4, "0") : ch;
  }
  return out;
}

/** Render one metadata field, or its fixed marker when the field is absent. */
function metaText(value, marker) {
  return typeof value === "string" ? escapeMeta(value) : marker;
}

/**
 * Render the trusted template once; braces and dollar signs in values stay literal.
 * @param {Record<string, string | number>} values Validated display-safe fields.
 * @returns {string} Notice text without further I/O or value interpretation.
 */
function renderTemplate(values) {
  return NOTICE_CONTRACT.template.replace(/\{([^}]+)\}/g, (match, key) => values[key] ?? match);
}

/** Return fixed trusted failure guidance without accepting runtime error prose. */
function failureBlock(status, failureKind) {
  if (!["failed", "timed_out", "lost"].includes(status)) return "";
  const kind = metaText(failureKind, "unknown");
  const classified = failureKind !== null && Object.hasOwn(NOTICE_CONTRACT.failure_guidance, failureKind)
    ? NOTICE_CONTRACT.failure_guidance[failureKind] : undefined;
  const guidance = status === "timed_out"
    ? NOTICE_CONTRACT.status_guidance.timed_out
    : classified || NOTICE_CONTRACT.status_guidance[status] || NOTICE_CONTRACT.default_failure;
  return `\n- Failure: ${kind} — ${guidance.reason}\n- Advice: ${guidance.advice}`;
}

/**
 * Validate an unknown v1/v2/v3 request and render fixed lifecycle text.
 * @param {unknown} request Decoded relay JSON containing no arbitrary prompt.
 * @returns {string} Escaped completion notice using notice contract v1.
 * @throws {Error} If keys, identifiers, metadata, version, or lifecycle are invalid.
 */
function notice(request) {
  if (!request || typeof request !== "object" || Array.isArray(request))
    throw new Error("invalid request");
  const keys = JSON.stringify(Object.keys(request).sort());
  const legacy = keys === JSON.stringify(LEGACY_KEYS);
  const v2 = keys === JSON.stringify(V2_KEYS), v3 = keys === JSON.stringify(V3_KEYS);
  const rich = v2 || v3;
  if (!legacy && !rich) throw new Error("invalid request");
  if (request.version !== (legacy ? 1 : v2 ? 2 : 3) || request.op !== "completion")
    throw new Error("invalid request");
  for (const key of ["agent_id", "notification_id", "thread_id", "status"])
    if (typeof request[key] !== "string" || !request[key].trim() || [...request[key]].length > 512 || request[key].includes("\0"))
      throw new Error("invalid identifier");
  if (rich)
    for (const key of ["effort", "model", "runtime"])
      if (request[key] !== null &&
          (typeof request[key] !== "string" || !request[key].trim() || [...request[key]].length > META_LIMIT))
        throw new Error("invalid metadata");
  if (v3 && request.failure_kind !== null &&
      (typeof request.failure_kind !== "string" || !request.failure_kind.trim() ||
       [...request.failure_kind].length > META_LIMIT))
    throw new Error("invalid metadata");
  if (!/^ag-\d{8}-\d{6}-[0-9a-f]{10}$/.test(request.agent_id) ||
      !/^ntf_[A-Za-z0-9_-]+$/.test(request.notification_id) ||
      !["succeeded", "failed", "timed_out", "cancelled", "lost"].includes(request.status) ||
      (v3 && request.failure_kind !== null && ["succeeded", "cancelled"].includes(request.status)))
    throw new Error("invalid lifecycle");
  return renderTemplate({
    agent_id: request.agent_id,
    status: request.status,
    failure_block: failureBlock(request.status, v3 ? request.failure_kind : null),
    runtime: metaText(request.runtime, "unknown"),
    model: metaText(request.model, "unknown"),
    effort: metaText(request.effort, "unspecified"),
    notification_id: request.notification_id,
    version: COMPLETION_NOTICE_VERSION,
  });
}

/** Deliver one validated notice; only a pre-call failure remains retryable. */
async function deliver(request) {
  let sent = false;
  const socket = new net.Socket(), deadline = Date.now() + HOST_MS;
  try {
    const prompt = notice(request);
    await connect(socket, pipe, deadline);
    const listed = await rpc(socket, 1, "tools/list", { threadStartKind: "all" }, deadline);
    const tool = Array.isArray(listed.tools) && listed.tools.find(
      item => item.name === "send_message_to_thread" && typeof item.namespace === "string" && item.namespace);
    if (!tool) return "rejected";
    sent = true;
    const result = await rpc(socket, 2, "tools/call", {
      arguments: { threadId: request.thread_id, prompt },
      callId: request.notification_id, namespace: tool.namespace,
      threadId: request.thread_id, tool: tool.name, turnId: request.notification_id,
    }, deadline);
    return result.success === true ? "accepted" : result.success === false ? "rejected" : "ambiguous";
  } catch (_) { return sent ? "ambiguous" : "rejected"; }
  finally { socket.destroy(); }
}

let active = 0;
/** Long-lived private relay server; each connection carries one bounded typed request. */
const server = net.createServer(async socket => {
  if (active >= MAX_CONNECTIONS) return socket.destroy();
  active += 1;
  try {
    const request = await receive(socket, LOCAL_LIMIT, Date.now() + HOST_MS);
    const outcome = await deliver(request);
    socket.end(frame({ outcome }, LOCAL_LIMIT));
  } catch (_) { socket.destroy(); }
  finally { active -= 1; }
});
let child, ownsPath = false, closing = false, childStarted = false;

/** Remove only the owned socket and optionally forward shutdown to the Rust MCP child. */
function cleanup(signal) {
  if (closing) return;
  closing = true;
  server.close();
  if (ownsPath) { try { fs.unlinkSync(relayPath); } catch (_) {} }
  if (signal && child) child.kill(signal);
}

/** Run the original Rust MCP with host capabilities removed and inherited protocol stdio. */
function startChild() {
  if (childStarted) return;
  childStarted = true;
  const env = { ...process.env };
  delete env.CODEX_APP_TOOLS_PIPE_PATH; delete env.CODEX_MCP_NODE_PATH;
  child = spawn(executable, mcpArgs, { env, stdio: "inherit" });
  child.once("error", () => { cleanup(); process.exit(1); });
  child.once("exit", (code, signal) => {
    cleanup();
    if (signal) process.kill(process.pid, signal);
    else process.exit(code === null ? 1 : code);
  });
}

server.once("error", () => {
  process.stderr.write("agent-run: Desktop relay unavailable; MCP continues without relay delivery\n");
  startChild();
});
try {
  fs.mkdirSync(home, { recursive: true, mode: 0o700 });
  const previous = process.umask(0o077);
  server.listen(relayPath, () => { ownsPath = true; fs.chmodSync(relayPath, 0o600); startChild(); });
  process.umask(previous);
} catch (_) { startChild(); }
for (const signal of ["SIGTERM", "SIGINT"])
  process.on(signal, () => { cleanup(signal); setTimeout(() => process.exit(1), 1000).unref(); });
process.on("exit", () => cleanup());
