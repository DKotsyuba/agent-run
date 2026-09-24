#!/usr/bin/env node
/** Readiness example for CodeGraph 1.6.0's private daemon protocol. No daemon is spawned. */
'use strict';
const fs = require('node:fs');
const net = require('node:net');
const path = require('node:path');
const {pathToFileURL} = require('node:url');

/**
 * Verify the configured PID/version and complete a real MCP status call.
 * @returns {void} Exits zero only for the expected ready daemon; otherwise exits one.
 */
function main() {
  const project = process.argv[2];
  const version = process.argv[3] || '1.6.0';
  const expected = Number(process.env.AGENT_RUN_SERVICE_PID);
  if (!project || !path.isAbsolute(project) || !Number.isSafeInteger(expected) || expected <= 1) {
    process.exitCode = 1;
    return;
  }
  let info;
  try { info = JSON.parse(fs.readFileSync(path.join(project, '.codegraph', 'daemon.pid'), 'utf8')); }
  catch { process.exitCode = 1; return; }
  if (!info || info.pid !== expected || typeof info.socketPath !== 'string') { process.exitCode = 1; return; }
  const socket = net.createConnection(info.socketPath);
  let buffer = '';
  let hello = false;
  let finished = false;
  const timer = setTimeout(() => finish(false), 4000);

  /** @param {boolean} ok Whether identity and the status call succeeded. @returns {void} */
  function finish(ok) {
    if (finished) return;
    finished = true;
    clearTimeout(timer);
    socket.destroy();
    process.exitCode = ok ? 0 : 1;
  }
  socket.setEncoding('utf8');
  socket.on('error', () => finish(false));
  socket.on('end', () => finish(false));
  socket.on('data', chunk => {
    buffer += chunk;
    if (buffer.length > 1024 * 1024) return finish(false);
    let boundary;
    while ((boundary = buffer.indexOf('\n')) >= 0) {
      let message;
      try { message = JSON.parse(buffer.slice(0, boundary)); }
      catch { return finish(false); }
      buffer = buffer.slice(boundary + 1);
      if (!hello) {
        if (message.pid !== expected || message.codegraph !== version || message.protocol !== 1) return finish(false);
        hello = true;
        socket.write([
          {codegraph_client: 1, pid: process.pid, hostPid: process.ppid},
          {jsonrpc: '2.0', id: 1, method: 'initialize', params: {
            protocolVersion: '2024-11-05', capabilities: {}, clientInfo: {name: 'agent-run-readiness', version: '1'}, rootUri: pathToFileURL(project).href,
          }},
        ].map(value => JSON.stringify(value)).join('\n') + '\n');
      } else if (message.id === 1) {
        if (message.error || !message.result) return finish(false);
        socket.write([
          {jsonrpc: '2.0', method: 'notifications/initialized'},
          {jsonrpc: '2.0', id: 2, method: 'tools/call', params: {name: 'codegraph_status', arguments: {projectPath: project}}},
        ].map(value => JSON.stringify(value)).join('\n') + '\n');
      } else if (message.id === 2) {
        return finish(!message.error && !!message.result && Array.isArray(message.result.content) && message.result.content.length > 0 && message.result.isError !== true);
      }
    }
  });
}
main();
