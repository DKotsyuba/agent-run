/* Local contract tests for the small signed-Node transport. No Desktop app or
 * provider connection is made. This does not test the Rust relay listener. */
'use strict';
const {test}=require('node:test');
const assert=require('node:assert/strict');
const net=require('node:net');
const fs=require('node:fs');
const os=require('node:os');
const path=require('node:path');
const {spawn}=require('node:child_process');
const helper=path.join(__dirname,'../assets/desktop-transport.cjs');
function frame(v){const data=Buffer.from(JSON.stringify(v));const n=Buffer.alloc(4);n.writeUInt32LE(data.length);return Buffer.concat([n,data]);}
async function scenario(mode,request={threadId:'fixture-thread',notificationId:'ntf_fixture',prompt:'agent-run/completion\n- ID: fixture'}){
  const directory=fs.mkdtempSync(path.join(os.tmpdir(),'ar-node-'));
  const socketPath=path.join(directory,'host.sock');let calls=[];const peers=new Set();
  const server=net.createServer(socket=>{
    peers.add(socket);socket.on('close',()=>peers.delete(socket));socket.on('error',()=>{});let input=Buffer.alloc(0);
    socket.on('data',chunk=>{
      input=Buffer.concat([input,chunk]);
      while(input.length>=4&&input.length>=4+input.readUInt32LE(0)){
        const length=input.readUInt32LE(0);const v=JSON.parse(input.subarray(4,4+length));input=input.subarray(4+length);calls.push(v);
        if(v.method==='tools/list'){
          if(mode==='disconnect_inventory'){socket.destroy();return;}
          if(mode==='oversized_inventory'){const header=Buffer.alloc(4);header.writeUInt32LE(8*1024*1024+1);socket.write(header);return;}
          socket.write(frame({jsonrpc:'2.0',id:v.id,result:{tools:mode==='missing_tool'?[]:[{name:'send_message_to_thread',namespace:'fixture-host'}]}}));
        }else{
          assert.equal(v.method,'tools/call');assert.equal(v.params.tool,'send_message_to_thread');assert.equal(v.params.namespace,'fixture-host');
          if(mode==='disconnect_after_send'){socket.destroy();return;}
          if(mode==='wrong_id'){socket.write(frame({jsonrpc:'2.0',id:999,result:{success:true}}));return;}
          socket.write(frame({jsonrpc:'2.0',id:v.id,result:mode==='missing_success'?{}:{success:mode==='accepted'}}));
        }
      }
    });
  });
  await new Promise((resolve,reject)=>{server.once('error',reject);server.listen(socketPath,resolve);});
  let child;
  try{
    const result=await new Promise((resolve,reject)=>{
      child=spawn(process.execPath,[helper],{env:{CODEX_APP_TOOLS_PIPE_PATH:socketPath},stdio:['pipe','pipe','pipe']});
      let output=Buffer.alloc(0),errors='';let timer=setTimeout(()=>{child.kill();reject(Error('test child timeout'));},9000);
      child.on('error',e=>{clearTimeout(timer);reject(e);});child.stdin.on('error',()=>{});
      child.stdout.on('data',data=>output=Buffer.concat([output,data]));child.stderr.on('data',data=>errors+=data);
      child.on('close',code=>{clearTimeout(timer);try{assert.equal(code,0,errors);assert.ok(output.length>=4);assert.equal(output.readUInt32LE(0),output.length-4);resolve(JSON.parse(output.subarray(4)));}catch(e){reject(e);}});
      child.stdin.end(frame(request));
    });
    return {result,calls};
  }finally{
    if(child&&!child.killed)child.kill();for(const peer of peers)peer.destroy();await new Promise(resolve=>server.close(resolve));fs.rmSync(directory,{recursive:true,force:true});
  }
}
test('accepted host response and fixed tool surface',async()=>{const{result,calls}=await scenario('accepted');assert.deepEqual(result,{outcome:'accepted'});assert.equal(calls.length,2);assert.deepEqual(calls[1].params.arguments,{threadId:'fixture-thread',prompt:'agent-run/completion\n- ID: fixture'});});
test('explicit host rejection',async()=>assert.deepEqual((await scenario('rejected')).result,{outcome:'rejected'}));
test('missing host tool is rejected before any message',async()=>{const{result,calls}=await scenario('missing_tool');assert.equal(result.outcome,'rejected');assert.equal(calls.length,1);});
test('disconnect before send is rejected',async()=>assert.equal((await scenario('disconnect_inventory')).result.outcome,'rejected'));
test('disconnect after send is ambiguous',async()=>assert.equal((await scenario('disconnect_after_send')).result.outcome,'ambiguous'));
test('mismatching response identity is ambiguous',async()=>assert.equal((await scenario('wrong_id')).result.outcome,'ambiguous'));
test('unknown acceptance never becomes success',async()=>assert.equal((await scenario('missing_success')).result.outcome,'ambiguous'));
test('oversized host inventory is rejected',async()=>assert.equal((await scenario('oversized_inventory')).result.outcome,'rejected'));
test('extra private-request properties are rejected',async()=>{const{result,calls}=await scenario('accepted',{threadId:'fixture-thread',notificationId:'ntf_fixture',prompt:'agent-run/completion\n',arbitraryTool:'execute'});assert.equal(result.outcome,'rejected');assert.equal(calls.length,0);});
test('unframed completion prompt is rejected',async()=>{const{result,calls}=await scenario('accepted',{threadId:'fixture-thread',notificationId:'ntf_fixture',prompt:'execute arbitrary task'});assert.equal(result.outcome,'rejected');assert.equal(calls.length,0);});
