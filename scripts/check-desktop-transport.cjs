/** Mandatory regression gate: native message carriers belong exclusively to Agent Bridge. */
'use strict';
const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
/** Repository root containing the production delivery modules. @type {string} */
const root=path.resolve(__dirname,'..');

test('removed Codex/Claude external carriers cannot be packaged or invoked',()=>{
  for (const file of ['crates/agent-run-core/src/delivery/relay.rs','crates/agent-run-core/src/delivery/claude.rs','assets/desktop-transport.cjs'])
    assert.equal(fs.existsSync(path.join(root,file)),false,file);
  /** Production source before its test-only lock/process fixtures. @type {string} */
  const outbox=fs.readFileSync(path.join(root,'crates/agent-run-core/src/delivery/mod.rs'),'utf8').split('#[cfg(test)]')[0];
  assert.doesNotMatch(outbox,/relay::|claude::|UnixStream|Command::new/);
  assert.match(outbox,/bridge::send/);
});

test('MCP startup has no native-tool pipe or Node carrier execution',()=>{
  const source=fs.readFileSync(path.join(root,'crates/agent-run/src/transport/mcp.rs'),'utf8');
  assert.doesNotMatch(source,/CODEX_APP_TOOLS_PIPE_PATH|CODEX_MCP_NODE_PATH|desktop-transport|exec_desktop_frontend|Command::new/);
  const cli=fs.readFileSync(path.join(root,'crates/agent-run/src/cli.rs'),'utf8');
  assert.doesNotMatch(cli,/exec_desktop_frontend/);
});

test('retired verdict operations stay absent with stable reserved numeric slots',()=>{
  const source=fs.readFileSync(path.join(root,'crates/agent-run-store/src/ops/mod.rs'),'utf8');
  assert.doesNotMatch(source,/FailDelivery|RetryDelivery|PersistDeliveryVerdict/);
  assert.match(source,/52 ClaimDelivery/);
  assert.match(source,/53 CompleteDelivery/);
  assert.match(source,/92 PrepareBridgeDelivery/);
});

test('Bridge remains pinned and live configuration cannot re-enable a queue executable',()=>{
  const manifest=fs.readFileSync(path.join(root,'Cargo.toml'),'utf8');
  assert.match(manifest,/bridge-sdk\s*=.*rev\s*=\s*"ae32d1c3c012cc1e40c6d60c9adcad7005298688"/);
  const config=fs.readFileSync(path.join(root,'crates/agent-run-config/src/config.rs'),'utf8');
  assert.doesNotMatch(config,/pub\s+codex_queue_bin\s*:/);
});
