/* Minimal signed-Node Desktop transport. All lifecycle validation, notice
 * rendering, queueing, retries, and durable state live in Rust.
 * This helper intentionally has no arbitrary method or tool dispatch surface. */
"use strict";
const net = require("node:net");
const LIMIT = 8 * 1024 * 1024;
function frame(value, limit = LIMIT) {
  const body = Buffer.from(JSON.stringify(value));
  if (!body.length || body.length > limit) throw Error("frame bound");
  const head = Buffer.alloc(4); head.writeUInt32LE(body.length);
  return Buffer.concat([head, body]);
}
function receive(stream, limit) {
  return new Promise((resolve, reject) => {
    let data = Buffer.alloc(0), size;
    const done = (err, result) => {
      stream.off("data", onData); stream.off("error", onError);
      stream.off("end", onEnd); stream.off("close", onEnd);
      err ? reject(err) : resolve(result);
    };
    const onError = () => done(Error("transport"));
    const onEnd = () => done(Error("closed"));
    const onData = chunk => {
      if (data.length + chunk.length > limit + 4) return done(Error("frame bound"));
      data = Buffer.concat([data, chunk]);
      if (size === undefined && data.length >= 4) {
        size = data.readUInt32LE(0);
        if (!size || size > limit) return done(Error("frame bound"));
      }
      if (size !== undefined && data.length >= size + 4) {
        try { done(null, JSON.parse(data.subarray(4, size + 4).toString("utf8"))); }
        catch (_) { done(Error("JSON")); }
      }
    };
    stream.on("data", onData); stream.once("error", onError);
    stream.once("end", onEnd); stream.once("close", onEnd);
  });
}
let sent = false, socket;
let ended = false;
function finish(outcome) {
  if (ended) return; ended = true;
  if (socket) socket.destroy();
  process.stdout.end(frame({outcome}, 8192));
}
const timer = setTimeout(() => finish(sent ? "ambiguous" : "rejected"), 7500);
(async () => {
  const request = await receive(process.stdin, 8192);
  if (!request || Object.keys(request).sort().join(",") !== "notificationId,prompt,threadId" ||
      ["notificationId", "prompt", "threadId"].some(k => typeof request[k] !== "string") ||
      !request.notificationId.startsWith("ntf_") || !request.prompt.startsWith("agent-run/completion\n"))
    throw Error("invalid private request");
  socket = net.createConnection(process.env.CODEX_APP_TOOLS_PIPE_PATH);
  await new Promise((resolve,reject) => { socket.once("connect",resolve); socket.once("error",reject); });
  async function rpc(id, method, params) {
    const result = receive(socket, LIMIT);
    socket.write(frame({jsonrpc:"2.0",id,method,params}));
    const value = await result;
    if (!value || value.jsonrpc !== "2.0" || value.id !== id || "error" in value || !value.result)
      throw Error("invalid host envelope");
    return value.result;
  }
  const inventory = await rpc(1,"tools/list",{threadStartKind:"all"});
  const tool = Array.isArray(inventory.tools) && inventory.tools.find(t =>
    t.name === "send_message_to_thread" && typeof t.namespace === "string" && t.namespace);
  if (!tool) return finish("rejected");
  sent = true;
  const result = await rpc(2,"tools/call",{
    arguments:{threadId:request.threadId,prompt:request.prompt},callId:request.notificationId,
    namespace:tool.namespace,threadId:request.threadId,tool:tool.name,turnId:request.notificationId
  });
  finish(result.success === true ? "accepted" : result.success === false ? "rejected" : "ambiguous");
})().catch(() => finish(sent ? "ambiguous" : "rejected")).finally(() => clearTimeout(timer));
