/** Bounded socket fixtures for owned and external CodeGraph readiness; no daemon or model is started. */
'use strict';
const assert = require('node:assert/strict');
const {test} = require('node:test');
const fs = require('node:fs/promises');
const net = require('node:net');
const path = require('node:path');
const {execFile} = require('node:child_process');

test('readiness accepts external endpoints while preserving owned PID and protocol checks', {timeout: 15000}, async t => {
  const root = await fs.mkdtemp('/tmp/ar-cg-probe-');
  const socketPath = path.join(root, 'daemon.sock');
  const clients = new Set();
  const pid = process.pid;
  const server = net.createServer(socket => {
    clients.add(socket);
    socket.on('close', () => clients.delete(socket));
    socket.on('error', () => {});
    socket.setEncoding('utf8');
    socket.write(JSON.stringify({pid, protocol: 1, codegraph: '1.6.0'}) + '\n');
    let pending = '';
    socket.on('data', data => {
      pending += data;
      let boundary;
      while ((boundary = pending.indexOf('\n')) >= 0) {
        const message = JSON.parse(pending.slice(0, boundary));
        pending = pending.slice(boundary + 1);
        if (message.id) socket.write(JSON.stringify({jsonrpc: '2.0', id: message.id,
          result: message.id === 1 ? {} : {content: [{type: 'text', text: 'ready'}]}}) + '\n');
      }
    });
  });
  t.after(async () => {
    for (const client of clients) client.destroy();
    await new Promise(resolve => server.close(resolve));
    await fs.rm(root, {recursive: true, force: true});
  });
  await fs.mkdir(path.join(root, '.codegraph'));
  await fs.writeFile(path.join(root, '.codegraph', 'daemon.pid'), JSON.stringify({pid, socketPath}));
  await new Promise((resolve, reject) => { server.once('error', reject); server.listen(socketPath, resolve); });

  /** @param {string|undefined} mode Probe ownership mode. @param {number|undefined} expected Owned PID.
   * @param {string} version Expected protocol version. @returns {Promise<number>} Child exit status within two seconds. */
  function check(mode, expected, version = '1.6.0') {
    const env = {...process.env};
    delete env.AGENT_RUN_SERVICE_OWNERSHIP;
    delete env.AGENT_RUN_SERVICE_PID;
    if (mode !== undefined) env.AGENT_RUN_SERVICE_OWNERSHIP = mode;
    if (expected !== undefined) env.AGENT_RUN_SERVICE_PID = String(expected);
    return new Promise((resolve, reject) => execFile(process.execPath,
      [path.join(__dirname, 'services/codegraph-probe.cjs'), root, version],
      {env, timeout: 2000, killSignal: 'SIGKILL'}, error => {
        if (error && typeof error.code !== 'number') reject(error);
        else resolve(error ? error.code : 0);
      }));
  }
  assert.equal(await check('external', undefined), 0);
  assert.equal(await check('managed', pid), 0);
  assert.equal(await check(undefined, pid), 0, 'legacy owned probe remains supported');
  assert.equal(await check('managed', pid + 1), 1);
  assert.equal(await check('managed', undefined), 1);
  assert.equal(await check('unknown', pid), 1);
  assert.equal(await check('external', undefined, 'wrong-version'), 1);
});
