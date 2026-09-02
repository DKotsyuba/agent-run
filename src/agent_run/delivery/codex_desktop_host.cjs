/** Optional signed-Node MCP host: fixed completion notices, never arbitrary RPC. */
"use strict";
const fs = require("node:fs");
const net = require("node:net");
const path = require("node:path");
const { spawn } = require("node:child_process");
/** Host inventory is larger than the small local delivery request. */
/** Frame limits in bytes and host deadline in ms, below the client's 10 s budget. */
const LOCAL_LIMIT = 8192, HOST_LIMIT = 8 * 1024 * 1024, HOST_MS = 8000;
const [python, home, ...pythonArgs] = process.argv.slice(2);
const pipe = process.env.CODEX_APP_TOOLS_PIPE_PATH;
const relayPath = path.join(home, `ar-cdx-${process.pid}.sock`);

/** Encode a bounded JSON object as a uint32-LE frame. */
function frame(value, limit) {
  const body = Buffer.from(JSON.stringify(value));
  if (!body.length || body.length > limit) throw new Error("frame limit");
  const header = Buffer.alloc(4);
  header.writeUInt32LE(body.length);
  return Buffer.concat([header, body]);
}

/** Read one frame before deadline; remove every listener when settled. */
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

/** Connect before the same overall deadline; no completion has been sent yet. */
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

/** Accept only typed lifecycle facts and reproduce CompletionNotice.render exactly. */
function notice(request) {
  const keys = ["agent_id", "notification_id", "op", "status", "thread_id", "version"];
  if (!request || typeof request !== "object" || Array.isArray(request) ||
      JSON.stringify(Object.keys(request).sort()) !== JSON.stringify(keys) ||
      request.version !== 1 || request.op !== "completion") throw new Error("invalid request");
  for (const key of ["agent_id", "notification_id", "thread_id", "status"])
    if (typeof request[key] !== "string" || !request[key].trim() || request[key].length > 512 || request[key].includes("\0"))
      throw new Error("invalid identifier");
  if (!/^ag-\d{8}-\d{6}-[0-9a-f]{10}$/.test(request.agent_id) ||
      !/^ntf_[A-Za-z0-9_-]+$/.test(request.notification_id) ||
      !["succeeded", "failed", "timed_out", "cancelled", "lost"].includes(request.status))
    throw new Error("invalid lifecycle");
  return `agent-run: agent ${request.agent_id} finished with status ${request.status}. ` +
    `Call summary(${request.agent_id}) or transcript(${request.agent_id}) for details. ` +
    `Do not start a replacement agent for this notification. ` +
    `[notification ${request.notification_id} v1]`;
}

/** Only a pre-call failure permits queue fallback; uncertain calls stay ambiguous. */
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

/** One bounded local request per connection; protocol output never reaches MCP stdout. */
const server = net.createServer(async socket => {
  try {
    const request = await receive(socket, LOCAL_LIMIT, Date.now() + HOST_MS);
    const outcome = await deliver(request);
    socket.end(frame({ outcome }, LOCAL_LIMIT));
  } catch (_) { socket.destroy(); }
});
let child, ownsPath = false, closing = false;

/** Remove only this process's socket and forward shutdown to its Python child. */
function cleanup(signal) {
  if (closing) return;
  closing = true;
  server.close();
  if (ownsPath) { try { fs.unlinkSync(relayPath); } catch (_) {} }
  if (signal && child) child.kill(signal);
}

/** Run the original thin Python MCP with host capabilities removed: no recursive wrapper. */
function startChild() {
  const env = { ...process.env };
  delete env.CODEX_APP_TOOLS_PIPE_PATH; delete env.CODEX_MCP_NODE_PATH;
  child = spawn(python, pythonArgs, { env, stdio: "inherit" });
  child.once("error", () => { cleanup(); process.exit(1); });
  child.once("exit", code => { cleanup(); process.exit(code === null ? 1 : code); });
}
server.once("error", () => { process.stderr.write("agent-run: Desktop relay unavailable; queue fallback remains active\n"); startChild(); });
try {
  fs.mkdirSync(home, { recursive: true, mode: 0o700 });
  const previous = process.umask(0o077);
  server.listen(relayPath, () => { ownsPath = true; fs.chmodSync(relayPath, 0o600); startChild(); });
  process.umask(previous);
} catch (_) { startChild(); }
for (const signal of ["SIGTERM", "SIGINT"])
  process.on(signal, () => { cleanup(signal); setTimeout(() => process.exit(1), 1000).unref(); });
process.on("exit", () => cleanup());
