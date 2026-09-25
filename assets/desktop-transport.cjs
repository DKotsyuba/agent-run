/** Signed-Node MCP frontend: fixed completion notices, never arbitrary RPC. */
"use strict";
const crypto = require("node:crypto");
const fs = require("node:fs");
const net = require("node:net");
const path = require("node:path");
const { spawn } = require("node:child_process");

/** Frame limits, host deadline, and local concurrency ceiling. @type {number} */
const LOCAL_LIMIT = 8192, HOST_LIMIT = 8 * 1024 * 1024, HOST_MS = 8000, MAX_CONNECTIONS = 8;
/** Rust executable, private home, embedded notice contract, and exact original MCP arguments. @type {[string, string, string, ...string[]]} */
const [executable, home, contractJson, ...mcpArgs] = process.argv.slice(1);
if (!path.isAbsolute(executable || "") || !path.isAbsolute(home || "") || !contractJson)
  throw new Error("invalid frontend arguments");
/** Desktop native-tools pipe inherited only by this signed frontend. @type {string | undefined} */
const pipe = process.env.CODEX_APP_TOOLS_PIPE_PATH;
/** Private relay endpoint with a random suffix so PID reuse cannot collide. @type {string} */
const relayPath = path.join(home, `ar-cdx-v4-${process.pid}-${crypto.randomBytes(3).toString("hex")}.sock`);
/** Trusted completion template embedded by the Rust executable. @type {{template: string, status_guidance: Record<string, {reason: string, advice: string}>, failure_guidance: Record<string, {reason: string, advice: string}>, default_failure: {reason: string, advice: string}}} */
const NOTICE_CONTRACT = JSON.parse(contractJson);
/** Notice marker version, independent of the accepted relay wire versions. @type {number} */
const COMPLETION_NOTICE_VERSION = 1;
/** Exact request shapes for legacy v1, selector-rich v2, and failure-aware v3. @type {string[]} */
const LEGACY_KEYS = ["agent_id", "notification_id", "op", "status", "thread_id", "version"];
/** Selector-rich v2 request keys. @type {string[]} */
const V2_KEYS = ["agent_id", "effort", "model", "notification_id", "op", "runtime", "status", "thread_id", "version"];
/** Failure-aware v3 request keys. @type {string[]} */
const V3_KEYS = ["agent_id", "effort", "failure_kind", "model", "notification_id", "op", "runtime", "status", "thread_id", "version"];
/** Stable-agent and exact-run v4 request keys. @type {string[]} */
const V4_KEYS = ["agent_id", "effort", "failure_kind", "model", "notification_id", "op", "run_id", "runtime", "status", "thread_id", "version"];
/** Launch metadata bound in code points, matching the Rust notice contract. @type {number} */
const META_LIMIT = 128;

/**
 * Encode a bounded JSON value as a uint32-LE frame.
 * @param {unknown} value JSON-serializable value to frame.
 * @param {number} limit Maximum encoded body bytes, exclusive of the header.
 * @returns {Buffer} Header and encoded body.
 * @throws {Error} When the encoded body is empty or exceeds `limit`.
 */
function frame(value, limit) {
  const body = Buffer.from(JSON.stringify(value));
  if (!body.length || body.length > limit) throw new Error("frame limit");
  const header = Buffer.alloc(4);
  header.writeUInt32LE(body.length);
  return Buffer.concat([header, body]);
}

/**
 * Read one bounded JSON frame before an absolute millisecond deadline.
 * @param {net.Socket} socket Connected stream that supplies exactly one frame.
 * @param {number} limit Maximum accepted body bytes.
 * @param {number} deadline Unix time in milliseconds when the read expires.
 * @returns {Promise<unknown>} Decoded JSON value.
 * @throws {Error} On timeout, closure, transport failure, malformed JSON, or an invalid bound.
 */
function receive(socket, limit, deadline) {
  return new Promise((resolve, reject) => {
    /** Buffered frame bytes received so far. @type {Buffer} */
    let data = Buffer.alloc(0);
    /** Declared body length once its header arrives. @type {number | undefined} */
    let size;
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

/**
 * Connect a socket before the shared absolute deadline.
 * @param {net.Socket} socket Unconnected socket to open.
 * @param {string} endpoint Absolute native-pipe or Unix-socket path.
 * @param {number} deadline Unix time in milliseconds when connection expires.
 * @returns {Promise<void>} Resolution after connection.
 * @throws {Error} On timeout or transport failure.
 */
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

/**
 * Exchange one correlated native-host JSON-RPC request.
 * @param {net.Socket} socket Connected native-host socket.
 * @param {number} id Fixed request correlation identifier.
 * @param {string} method Native-host method name selected by this frontend.
 * @param {Record<string, unknown>} params Fixed method parameters.
 * @param {number} deadline Shared absolute millisecond deadline.
 * @returns {Promise<Record<string, unknown>>} Validated result object.
 * @throws {Error} On transport, framing, correlation, or host-envelope failure.
 */
async function rpc(socket, id, method, params, deadline) {
  const reply = receive(socket, HOST_LIMIT, deadline);
  socket.write(frame({ jsonrpc: "2.0", id, method, params }, HOST_LIMIT));
  const value = await reply;
  if (!value || value.jsonrpc !== "2.0" || value.id !== id || "error" in value || !value.result)
    throw new Error("invalid response");
  return value.result;
}

/**
 * Escape control and Unicode line-separator code points exactly like Rust.
 * @param {string} value Validated metadata text.
 * @returns {string} Display-safe text with affected code points rendered as `\\uXXXX`.
 */
function escapeMeta(value) {
  let out = "";
  for (const ch of value) {
    const code = ch.codePointAt(0);
    out += code < 0x20 || (code >= 0x7f && code <= 0x9f) || code === 0x2028 || code === 0x2029
      ? "\\u" + code.toString(16).padStart(4, "0") : ch;
  }
  return out;
}

/**
 * Render one metadata field or its fixed missing-value marker.
 * @param {string | null | undefined} value Validated metadata or absence.
 * @param {string} marker Trusted absence marker.
 * @returns {string} Escaped metadata or `marker`.
 */
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

/**
 * Return fixed trusted failure guidance without accepting runtime error prose.
 * @param {string} status Validated terminal lifecycle status.
 * @param {string | null} failureKind Validated classifier or absence.
 * @returns {string} Empty text for non-failures or a prefixed failure block.
 */
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
 * Validate an unknown v1-v4 request and render fixed lifecycle text.
 * @param {unknown} request Decoded relay JSON containing no arbitrary prompt.
 * @returns {string} Escaped completion notice using notice contract v1.
 * @throws {Error} If keys, identifiers, metadata, version, or lifecycle are invalid.
 */
function notice(request) {
  if (!request || typeof request !== "object" || Array.isArray(request))
    throw new Error("invalid request");
  const keys = JSON.stringify(Object.keys(request).sort());
  const legacy = keys === JSON.stringify(LEGACY_KEYS);
  const v2 = keys === JSON.stringify(V2_KEYS), v3 = keys === JSON.stringify(V3_KEYS), v4 = keys === JSON.stringify(V4_KEYS);
  const rich = v2 || v3 || v4;
  if (!legacy && !rich) throw new Error("invalid request");
  if (request.version !== (legacy ? 1 : v2 ? 2 : v3 ? 3 : 4) || request.op !== "completion")
    throw new Error("invalid request");
  for (const key of ["agent_id", "notification_id", "thread_id", "status"])
    if (typeof request[key] !== "string" || !request[key].trim() || [...request[key]].length > 512 || request[key].includes("\0"))
      throw new Error("invalid identifier");
  if (rich)
    for (const key of ["effort", "model", "runtime"])
      if (request[key] !== null &&
          (typeof request[key] !== "string" || !request[key].trim() || [...request[key]].length > META_LIMIT))
        throw new Error("invalid metadata");
  if ((v3 || v4) && request.failure_kind !== null &&
      (typeof request.failure_kind !== "string" || !request.failure_kind.trim() ||
       [...request.failure_kind].length > META_LIMIT))
    throw new Error("invalid metadata");
  if (!/^ag-\d{8}-\d{6}-[0-9a-f]{10}$/.test(request.agent_id) ||
      (v4 && (typeof request.run_id !== "string" || !/^ag-\d{8}-\d{6}-[0-9a-f]{10}$/.test(request.run_id))) ||
      !/^ntf_[A-Za-z0-9_-]+$/.test(request.notification_id) ||
      !["succeeded", "failed", "timed_out", "cancelled", "lost"].includes(request.status) ||
      ((v3 || v4) && request.failure_kind !== null && ["succeeded", "cancelled"].includes(request.status)))
    throw new Error("invalid lifecycle");
  return renderTemplate({
    agent_id: request.agent_id,
    run_block: v4 ? `\n- Run: ${request.run_id}` : "",
    status: request.status,
    failure_block: failureBlock(request.status, (v3 || v4) ? request.failure_kind : null),
    runtime: metaText(request.runtime, "unknown"),
    model: metaText(request.model, "unknown"),
    effort: metaText(request.effort, "unspecified"),
    notification_id: request.notification_id,
    version: COMPLETION_NOTICE_VERSION,
  });
}

/**
 * Deliver one typed notice through the fixed native-host tool.
 * @param {Record<string, unknown>} request Validated only by `notice` before host contact.
 * @returns {Promise<"accepted" | "rejected" | "ambiguous">} Durable delivery classification.
 */
async function deliver(request) {
  /** Whether the native message call may have begun. @type {boolean} */
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

/** Number of local relay connections currently consuming bounded work. @type {number} */
let active = 0;
/** Long-lived private relay server; each connection carries one bounded typed request. @type {net.Server} */
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
/** Capability-stripped Rust MCP child after startup. @type {import("node:child_process").ChildProcess | undefined} */
let child;
/** Whether this process successfully bound and therefore owns `relayPath`. @type {boolean} */
let ownsPath = false;
/** Whether cleanup has already begun. @type {boolean} */
let closing = false;
/** Whether the single Rust MCP child start has been attempted. @type {boolean} */
let childStarted = false;

/**
 * Remove only the owned socket and optionally forward shutdown to the Rust MCP child.
 * @param {NodeJS.Signals | undefined} signal Signal to forward, or absence on normal child exit.
 * @returns {void}
 */
function cleanup(signal) {
  if (closing) return;
  closing = true;
  server.close();
  if (ownsPath) { try { fs.unlinkSync(relayPath); } catch (_) {} }
  if (signal && child) child.kill(signal);
}

/**
 * Run the original Rust MCP with host capabilities removed and inherited protocol stdio.
 * @returns {void}
 */
function startChild() {
  if (childStarted) return;
  childStarted = true;
  const env = { ...process.env };
  delete env.CODEX_APP_TOOLS_PIPE_PATH; delete env.CODEX_MCP_NODE_PATH;
  // The Rust child observes this parent relationship even if SIGKILL skips cleanup.
  env.AGENT_RUN_MCP_PARENT_PID = String(process.pid);
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
