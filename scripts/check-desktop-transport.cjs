/* Local contract tests for the long-lived signed-Node frontend. No Desktop app,
 * live relay, provider connection, or production notification is used. */
'use strict';
const {test}=require('node:test');
const assert=require('node:assert/strict');
const net=require('node:net');
const fs=require('node:fs');
const os=require('node:os');
const path=require('node:path');
const {spawn}=require('node:child_process');
/** Embedded frontend source exercised through Node's `-e` path. @type {string} */
const frontend=fs.readFileSync(path.join(__dirname,'../assets/desktop-transport.cjs'),'utf8');
/** Canonical notice contract supplied as the production argument. @type {string} */
const contract=fs.readFileSync(path.join(__dirname,'../assets/completion_notice.json'),'utf8');
/** Valid v3 request reused by behavior scenarios. @type {Record<string, unknown>} */
const valid={version:3,op:'completion',thread_id:'thread-fixture',notification_id:'ntf_fixture',agent_id:'ag-20260825-120000-0123456789',status:'succeeded',runtime:null,model:null,effort:null,failure_kind:null};

/**
 * Encode one fixture value using the production uint32-LE format.
 * @param {unknown} value JSON-serializable fixture value.
 * @returns {Buffer} Header and encoded body.
 */
function frame(value){const data=Buffer.from(JSON.stringify(value));const n=Buffer.alloc(4);n.writeUInt32LE(data.length);return Buffer.concat([n,data]);}

/**
 * Read one framed JSON reply from a fixture socket.
 * @param {net.Socket} socket Connected local relay socket.
 * @returns {Promise<unknown>} Decoded reply value; rejects on transport failure or early close.
 */
function receive(socket){return new Promise((resolve,reject)=>{/** @type {Buffer} */let data=Buffer.alloc(0);socket.on('data',chunk=>{data=Buffer.concat([data,chunk]);if(data.length>=4&&data.length>=4+data.readUInt32LE(0))resolve(JSON.parse(data.subarray(4,4+data.readUInt32LE(0))));});socket.once('error',reject);socket.once('close',()=>data.length<4&&reject(Error('closed')));});}

/**
 * Wait for the frontend's randomly suffixed private relay path.
 * @param {string} directory Private temporary home to scan.
 * @returns {Promise<string>} Absolute relay path after it appears.
 * @throws {Error} When no endpoint appears within the bounded polling window.
 */
async function relayPath(directory){for(let i=0;i<200;i++){const name=fs.readdirSync(directory).find(v=>v.startsWith('ar-cdx-v4-')&&v.endsWith('.sock'));if(name)return path.join(directory,name);await new Promise(resolve=>setTimeout(resolve,10));}throw Error('relay did not start');}

/**
 * Connect after the listening callback, tolerating the socket-file visibility race.
 * @param {string} endpoint Absolute relay socket path.
 * @returns {Promise<net.Socket>} Connected local relay socket.
 * @throws {Error} On non-race connection failures or exhausted retries.
 */
async function connectRelay(endpoint){for(let i=0;i<200;i++){try{return await new Promise((resolve,reject)=>{const socket=net.createConnection(endpoint);socket.once('connect',()=>resolve(socket));socket.once('error',reject);});}catch(error){if(error.code!=='ECONNREFUSED')throw error;await new Promise(resolve=>setTimeout(resolve,10));}}throw Error('relay did not accept');}

/**
 * Run one typed local request through the frontend and a fake native host.
 * @param {string} mode Fixture host response or connection-cap scenario.
 * @param {Record<string, unknown>} [request=valid] Typed relay request.
 * @returns {Promise<{result?: unknown, calls: Record<string, unknown>[], hostPid?: number, child?: Record<string, unknown>, overflowClosed?: boolean}>} Scenario observations after complete process and socket cleanup.
 * @throws {Error} On fixture startup, protocol, timeout, or cleanup failure.
 */
async function scenario(mode,request=valid){
  const directory=fs.mkdtempSync(path.join(os.tmpdir(),'ar-node-'));
  const socketPath=path.join(directory,'host.sock');const calls=[];const peers=new Set();
  const server=net.createServer(socket=>{peers.add(socket);socket.on('close',()=>peers.delete(socket));socket.on('error',()=>{});/** @type {Buffer} */let input=Buffer.alloc(0);socket.on('data',chunk=>{input=Buffer.concat([input,chunk]);while(input.length>=4&&input.length>=4+input.readUInt32LE(0)){const length=input.readUInt32LE(0);const value=JSON.parse(input.subarray(4,4+length));input=input.subarray(4+length);calls.push(value);if(value.method==='tools/list'){if(mode==='disconnect_inventory')return socket.destroy();if(mode==='oversized_inventory'){const head=Buffer.alloc(4);head.writeUInt32LE(8*1024*1024+1);return socket.write(head);}socket.write(frame({jsonrpc:'2.0',id:value.id,result:{tools:mode==='missing_tool'?[]:[{name:'other',namespace:'wrong'},{name:'send_message_to_thread',namespace:'fixture-host'}]}}));}else{assert.equal(value.method,'tools/call');assert.equal(value.params.tool,'send_message_to_thread');assert.equal(value.params.namespace,'fixture-host');if(mode==='disconnect_after_send')return socket.destroy();if(mode==='wrong_id')return socket.write(frame({jsonrpc:'2.0',id:999,result:{success:true}}));socket.write(frame({jsonrpc:'2.0',id:value.id,result:mode==='missing_success'?{}:{success:mode==='accepted'}}));}}});});
  await new Promise((resolve,reject)=>{server.once('error',reject);server.listen(socketPath,resolve);});
  /** Self-expiring fixture: parent loss exits within 250 ms and lifetime never exceeds 30 seconds. @type {string} */
  const childCode="process.stdout.write(JSON.stringify({pid:process.pid,ppid:process.ppid,pipe:process.env.CODEX_APP_TOOLS_PIPE_PATH||null,node:process.env.CODEX_MCP_NODE_PATH||null})+'\\n');const parent=process.ppid;setInterval(()=>{if(process.ppid===1||process.ppid!==parent)process.exit(0)},250);setTimeout(()=>process.exit(0),30000)";
  const host=spawn(process.execPath,['-e','setTimeout(()=>process.exit(1),45000).unref();\n'+frontend,'--',process.execPath,directory,contract,'-e',childCode],{env:{...process.env,CODEX_APP_TOOLS_PIPE_PATH:socketPath,CODEX_MCP_NODE_PATH:process.execPath},stdio:['ignore','pipe','pipe']});
  /** Child protocol stdout captured without frontend output. @type {string} */
  let output='';host.stdout.on('data',data=>output+=data);host.stderr.on('data',()=>{});
  try{
    const endpoint=await relayPath(directory);
    if(mode==='connection_cap'){const holders=await Promise.all(Array.from({length:8},()=>connectRelay(endpoint)));await new Promise(resolve=>setTimeout(resolve,20));const overflow=await connectRelay(endpoint);await new Promise((resolve,reject)=>{const timer=setTimeout(()=>reject(Error('overflow connection stayed open')),1000);overflow.once('close',()=>{clearTimeout(timer);resolve();});});for(const socket of holders)socket.destroy();return {overflowClosed:true,calls};}
    const client=await connectRelay(endpoint);const reply=receive(client);client.write(frame(request));const result=await reply;client.destroy();for(let i=0;i<100&&!output.includes('\n');i++)await new Promise(resolve=>setTimeout(resolve,10));const child=JSON.parse(output.trim());return {result,calls,hostPid:host.pid,child};
  }finally{
    host.kill('SIGTERM');await new Promise(resolve=>host.once('close',resolve));for(const peer of peers)peer.destroy();await new Promise(resolve=>server.close(resolve));assert.equal(fs.readdirSync(directory).some(v=>v.startsWith('ar-cdx-v4-')),false);fs.rmSync(directory,{recursive:true,force:true});
  }
}

test('accepted response uses only the fixed native tool and child protocol stdout',async()=>{const{result,calls,hostPid,child}=await scenario('accepted');assert.deepEqual(result,{outcome:'accepted'});assert.equal(calls.length,2);assert.equal(calls[1].params.tool,'send_message_to_thread');assert.equal(calls[1].params.arguments.threadId,valid.thread_id);assert.equal(child.ppid,hostPid);assert.notEqual(child.pid,hostPid);assert.equal(child.pipe,null);assert.equal(child.node,null);});
test('explicit host rejection',async()=>assert.deepEqual((await scenario('rejected')).result,{outcome:'rejected'}));
test('missing host tool is rejected before any message',async()=>{const{result,calls}=await scenario('missing_tool');assert.equal(result.outcome,'rejected');assert.equal(calls.length,1);});
test('disconnect before call is rejected',async()=>assert.equal((await scenario('disconnect_inventory')).result.outcome,'rejected'));
test('disconnect after call is ambiguous',async()=>assert.equal((await scenario('disconnect_after_send')).result.outcome,'ambiguous'));
test('mismatching response identity is ambiguous',async()=>assert.equal((await scenario('wrong_id')).result.outcome,'ambiguous'));
test('unknown acceptance never becomes success',async()=>assert.equal((await scenario('missing_success')).result.outcome,'ambiguous'));
test('oversized host inventory is rejected',async()=>assert.equal((await scenario('oversized_inventory')).result.outcome,'rejected'));
test('ninth concurrent local connection is refused',async()=>{const{overflowClosed,calls}=await scenario('connection_cap');assert.equal(overflowClosed,true);assert.equal(calls.length,0);});
test('extra typed-request properties are rejected before host contact',async()=>{const{result,calls}=await scenario('accepted',{...valid,prompt:'inject'});assert.equal(result.outcome,'rejected');assert.equal(calls.length,0);});
test('missing typed-request properties are rejected before host contact',async()=>{const malformed={...valid};delete malformed.failure_kind;const{result,calls}=await scenario('accepted',malformed);assert.equal(result.outcome,'rejected');assert.equal(calls.length,0);});
test('success with failure guidance is rejected before host contact',async()=>{const{result,calls}=await scenario('accepted',{...valid,failure_kind:'prepare_failed'});assert.equal(result.outcome,'rejected');assert.equal(calls.length,0);});
test('unsupported wire version is rejected before host contact',async()=>{const{result,calls}=await scenario('accepted',{...valid,version:5});assert.equal(result.outcome,'rejected');assert.equal(calls.length,0);});
test('relay bind failure still runs the capability-stripped MCP child',async()=>{const directory=fs.mkdtempSync(path.join(os.tmpdir(),'ar-node-fallback-'));const blockedHome=path.join(directory,'file');fs.writeFileSync(blockedHome,'x');const child=spawn(process.execPath,['-e',frontend,'--',process.execPath,blockedHome,contract,'-e',"process.stdout.write(JSON.stringify({pipe:process.env.CODEX_APP_TOOLS_PIPE_PATH||null,node:process.env.CODEX_MCP_NODE_PATH||null}))"],{env:{...process.env,CODEX_APP_TOOLS_PIPE_PATH:path.join(directory,'host.sock'),CODEX_MCP_NODE_PATH:process.execPath},stdio:['ignore','pipe','pipe']});let output='';child.stdout.on('data',data=>output+=data);const status=await new Promise((resolve,reject)=>{child.once('error',reject);child.once('close',resolve);});assert.equal(status,0);assert.deepEqual(JSON.parse(output),{pipe:null,node:null});fs.rmSync(directory,{recursive:true,force:true});});

/** The current relay preserves both identities in the rendered completion. */
test('v4 completion retains stable agent and exact run',async()=>{const request={...valid,version:4,run_id:'ag-20260925-000000-0000000002'};const{result,calls}=await scenario('accepted',request);assert.equal(result.outcome,'accepted');const prompt=calls[1].params.arguments.prompt;assert.ok(prompt.includes(`- ID: ${request.agent_id}\n- Run: ${request.run_id}\n`));});
/** A malformed execution selector cannot reach the native host. */
test('v4 malformed run identity is rejected before host contact',async()=>{const{result,calls}=await scenario('accepted',{...valid,version:4,run_id:'injected\ntext'});assert.equal(result.outcome,'rejected');assert.equal(calls.length,0);});
