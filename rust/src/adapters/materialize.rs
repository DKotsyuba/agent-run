use crate::{config::{Adapter,Auth,Config,Runtime},domain::StartRequest,error::invalid,fs::{self,Dir},profiles::Profile,Result,Error};
use serde::{Deserialize,Serialize};
use serde_json::{json,Value};
use std::{collections::BTreeMap,path::{Path,PathBuf},os::unix::fs::PermissionsExt};
pub const SNAPSHOT:&str=".agent-run-rust-snapshot.json";
#[derive(Debug,Clone,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot{
    pub version:u32,pub files:BTreeMap<String,String>,pub links:BTreeMap<String,String>,
    pub plugin_paths:Vec<PathBuf>,pub plugin_roots:BTreeMap<String,PathBuf>,
}
pub struct Publisher {pub root:PathBuf,dir:Dir,pub snapshot:Snapshot,total:usize}
impl Publisher {
    pub fn new(root:&Path)->Result<Self>{fs::private_dir(root)?;Ok(Self{root:root.into(),dir:Dir::open(root)?,snapshot:Snapshot{version:1,files:BTreeMap::new(),links:BTreeMap::new(),plugin_paths:vec![],plugin_roots:BTreeMap::new()},total:0})}
    pub fn file(&mut self,path:&str,bytes:&[u8],mode:u32)->Result<()> {
        self.total+=bytes.len();if self.total>256*1024*1024{return Err(invalid("generated assets exceed 256 MiB"));}
        self.dir.write(Path::new(path),bytes,mode)?;self.snapshot.files.insert(path.into(),fs::sha256(bytes));Ok(())
    }
    pub fn json(&mut self,path:&str,value:&Value)->Result<()> {let mut data=serde_json::to_vec_pretty(value)?;data.push(b'\n');self.file(path,&data,0o600)}
    pub fn link(&mut self,path:&str,target:&Path)->Result<()> {
        let real=target.canonicalize().map_err(|_|invalid("credential source does not exist"))?;
        if !real.is_file(){return Err(invalid("credential source must be a regular file"));}
        self.dir.symlink(&real,Path::new(path))?;
        self.snapshot.links.insert(path.into(),real.to_string_lossy().into_owned());Ok(())
    }
    pub fn tree(&mut self,source:&Path,prefix:&str,selected:Option<&[String]>)->Result<()> {
        let source=source.to_path_buf();let input=Dir::open(&source)?;
        fn walk(p:&mut Publisher,root:&Path,input:&Dir,relative:&Path,prefix:&str,selected:Option<&[String]>)->Result<()> {
            let mut entries=std::fs::read_dir(root.join(relative))?.collect::<std::result::Result<Vec<_>,_>>()?;entries.sort_by_key(|e|e.file_name());
            for entry in entries{
                let name=entry.file_name();if name==".git"||name=="__pycache__"{continue;}
                let rel=relative.join(name);let include=selected.map(|s|s.iter().any(|a|rel.starts_with(a)||Path::new(a).starts_with(&rel))).unwrap_or(true);
                if !include{continue;}
                let meta=std::fs::symlink_metadata(entry.path())?;
                if meta.file_type().is_symlink(){return Err(invalid("snapshot source contains a symlink"));}
                let output=format!("{prefix}/{}",rel.to_str().ok_or_else(||invalid("snapshot paths must be UTF-8"))?);
                if meta.is_dir(){p.dir.directory(Path::new(&output))?;walk(p,root,input,&rel,prefix,selected)?;}
                else if meta.is_file(){let bytes=input.read(&rel,16*1024*1024)?;p.file(&output,&bytes,if meta.permissions().mode()&0o111!=0{0o700}else{0o600})?;}
                else{return Err(invalid("snapshot source contains a special file"));}
            }Ok(())
        }
        if let Some(assets)=selected{for a in assets{fs::relative(Path::new(a))?;if !source.join(a).exists(){return Err(invalid("declared plugin asset is missing"));}}}
        self.dir.directory(Path::new(prefix))?;walk(self,&source,&input,Path::new(""),prefix,selected)
    }
    pub fn finish(self)->Result<(Snapshot,String)>{
        let data=serde_json::to_vec(&self.snapshot)?;let sha=fs::sha256(&data);
        self.dir.write(Path::new(SNAPSHOT),&data,0o600)?;Ok((self.snapshot,sha))
    }
}
pub fn verify(root:&Path,expected:&str)->Result<Snapshot>{
    let dir=Dir::open(root)?;let data=dir.read(Path::new(SNAPSHOT),1024*1024)?;
    if fs::sha256(&data)!=expected{return Err(Error::Integrity("runtime snapshot index was modified".into()));}
    let snapshot:Snapshot=serde_json::from_slice(&data)?;
    if snapshot.version!=1{return Err(invalid("unsupported runtime snapshot version"));}
    for(name,digest)in &snapshot.files{let b=dir.read(Path::new(name),16*1024*1024)?;if fs::sha256(&b)!=*digest{return Err(Error::Integrity("generated runtime artifact was modified".into()));}}
    for(name,target)in &snapshot.links{
        fs::relative(Path::new(name))?;let current=std::fs::read_link(root.join(name))?;
        if current!=Path::new(target){return Err(Error::Integrity("credential bridge target changed".into()));}
    }
    Ok(snapshot)
}
pub fn shell_quote(s:&str)->String{if !s.is_empty()&&s.bytes().all(|c|c.is_ascii_alphanumeric()||b"_@%+=:,./-".contains(&c)){s.into()}else{format!("'{}'",s.replace('\'',"'\"'\"'"))}}
pub fn shell_command(args:&[String])->String{args.iter().map(|s|shell_quote(s)).collect::<Vec<_>>().join(" ")}
pub fn resolve_executable(name:&str,path:&str)->Option<PathBuf>{path.split(':').filter(|s|!s.is_empty()).map(|p|Path::new(p).join(name)).find(|p|std::fs::metadata(p).map(|m|m.is_file()&&m.permissions().mode()&0o111!=0).unwrap_or(false))}
pub fn account_home(app_home:&Path,kind:Adapter,label:&str)->PathBuf{app_home.join("accounts").join(kind.name()).join(label)}
pub fn environment(config:&Config,runtime:&Runtime,profile:&Profile,home:&Path,account:Option<&str>,app_home:&Path)->Result<BTreeMap<String,String>>{
    let kind=runtime.kind()?;let mut env=BTreeMap::new();
    for k in ["PATH","USER","LOGNAME","SHELL","TMPDIR","TMP","TEMP","LANG","LC_ALL","LC_CTYPE","TERM","COLORTERM","TZ"]{if let Ok(v)=std::env::var(k){env.insert(k.into(),v);}}
    env.entry("PATH".into()).or_insert_with(||"/usr/local/bin:/usr/bin:/bin".into());
    let host_home=std::env::var("HOME").map_err(|_|invalid("HOME is unavailable"))?;
    env.insert("HOME".into(),if kind==Adapter::Claude&&account.is_none(){host_home}else{home.to_string_lossy().into_owned()});
    if kind==Adapter::Codex{env.insert("CODEX_HOME".into(),home.to_string_lossy().into_owned());}
    if kind==Adapter::Claude{if let Some(a)=account{env.insert("CLAUDE_CONFIG_DIR".into(),account_home(app_home,kind,a).join("claude-config").to_string_lossy().into_owned());}}
    let mut paths=Vec::new();
    if let Some(e)=runtime.environment.as_ref().and_then(|n|config.environments.get(n)){
        paths.extend(e.path.iter().map(|p|p.to_string_lossy().into_owned()));
        for(k,v)in &e.variables{
            if ["HOME","PATH","CODEX_HOME","CLAUDE_CONFIG_DIR","AGENT_RUN_HOME"].contains(&k.as_str())||["TOKEN","SECRET","PASSWORD","API_KEY","CREDENTIAL"].iter().any(|s|k.contains(s)){return Err(invalid("developer environment must not override private homes or embed credentials"));}
            env.insert(k.clone(),v.clone());
        }
    }
    let rust=runtime.rust.as_ref().or_else(||runtime.environment.as_ref().and_then(|n|config.environments.get(n)).and_then(|e|e.rust.as_ref()));
    if let Some(r)=rust{env.insert("RUSTUP_HOME".into(),r.rustup_home.to_string_lossy().into_owned());env.insert("CARGO_HOME".into(),home.join(".cargo").to_string_lossy().into_owned());paths.push(r.cargo_bin.to_string_lossy().into_owned());}
    if let Some(p)=runtime.binary.parent(){paths.push(p.to_string_lossy().into_owned());}
    paths.push(env.get("PATH").cloned().unwrap_or_default());env.insert("PATH".into(),paths.join(":"));
    if let Some(e)=runtime.environment.as_ref().and_then(|n|config.environments.get(n)){
        for command in &e.required_commands{if resolve_executable(command,&env["PATH"]).is_none(){return Err(invalid("required developer command is unavailable"));}}
        if !e.denied_commands.is_empty(){env.insert("PATH".into(),format!("{}:{}",home.join("command-refusals").display(),env["PATH"]));}
    }
    let mut names=Vec::new();
    if let Some(Auth::Environment{names:declared})=&runtime.auth{names.extend(declared.clone());}
    for name in &profile.mcp{names.extend(config.mcp[name].env_from.clone());}
    for name in names{
        if ["HOME","PATH","CODEX_HOME","CLAUDE_CONFIG_DIR","AGENT_RUN_HOME","RUSTUP_HOME","CARGO_HOME"].contains(&name.as_str()){return Err(invalid("auth/MCP environment forwarding cannot override managed environment controls"));}
        let value=std::env::var(&name).map_err(|_|invalid("declared authentication/MCP environment variable is missing"))?;if value.is_empty(){return Err(invalid("declared authentication/MCP environment variable is empty"));}env.insert(name,value);}
    match kind{
        Adapter::Glm=>{
            env.insert("ANTHROPIC_BASE_URL".into(),"https://api.z.ai/api/anthropic".into());
            if !env.contains_key("ANTHROPIC_AUTH_TOKEN")&&!env.contains_key("ANTHROPIC_API_KEY"){return Err(invalid("GLM requires declared ANTHROPIC_AUTH_TOKEN or ANTHROPIC_API_KEY"));}
        },
        Adapter::Qwen=>{
            for key in ["OPENAI_API_KEY","OPENAI_BASE_URL"]{if !env.contains_key(key){return Err(invalid("Qwen requires explicitly declared OPENAI_API_KEY and OPENAI_BASE_URL"));}}
        },_=>{}
    }
    // Never propagate host orchestration capabilities to a child agent.
    for key in ["CODEX_APP_TOOLS_PIPE_PATH","CODEX_MCP_NODE_PATH","CODEX_THREAD_ID","CLAUDE_SESSION_ID"]{env.remove(key);}
    Ok(env)
}
pub fn materialize(config:&Config,runtime:&Runtime,request:&StartRequest,profile:&Profile,home:&Path,app_home:&Path)->Result<(Snapshot,String)>{
    let kind=runtime.kind()?;let mut p=Publisher::new(home)?;
    let plugins=super::plugins::install(&mut p,runtime,kind)?;
    p.snapshot.plugin_paths=plugins.paths.clone();p.snapshot.plugin_roots=plugins.roots.clone();
    let skill_catalog=if profile.canonical{config.skills_dir().to_path_buf()}else{app_home.join("skills").join(&request.runtime)};
    for skill in &profile.skills{
        let mut source=skill_catalog.join(skill);
        for plugin in &runtime.plugins{let candidate=plugin.join("skills").join(skill);if candidate.join("SKILL.md").is_file(){source=candidate;break;}}
        if !source.join("SKILL.md").is_file(){return Err(invalid("declared skill is unavailable"));}
        let relative=if matches!(kind,Adapter::Claude|Adapter::Glm){format!("plugins/{skill}/skills/{skill}")}else{format!("skills/{skill}")};
        p.tree(&source,&relative,None)?;
        if matches!(kind,Adapter::Claude|Adapter::Glm){p.json(&format!("plugins/{skill}/.claude-plugin/plugin.json"),&json!({"name":skill,"version":"1.0.0"}))?;p.snapshot.plugin_paths.push(home.join("plugins").join(skill));}
    }
    let mut mcp=serde_json::Map::new();
    for name in &profile.mcp{
        let server=&config.mcp[name];let env:serde_json::Map<String,Value>=server.env_from.iter().map(|n|(n.clone(),json!(format!("${{{n}}}")))).collect();
        let mut entry=json!({"command":server.command,"args":server.args});if !env.is_empty(){entry["env"]=Value::Object(env);}
        if kind==Adapter::Qwen{entry["trust"]=json!(true);}mcp.insert(name.clone(),entry);
    }
    let hooks=super::plugins::hook_groups(runtime,&plugins.roots)?;
    let native:Value=serde_json::to_value(&runtime.native_settings)?;
    match kind{
        Adapter::Codex=>{
            let mut doc:toml::Table=runtime.native_settings.clone().into_iter().collect();
            doc.entry("model_context_window").or_insert(toml::Value::Integer(1_000_000));
            doc.entry("model_auto_compact_token_limit").or_insert(toml::Value::Integer(780_000));
            doc.entry("model_auto_compact_token_limit_scope").or_insert(toml::Value::String("total".into()));
            doc.insert("cli_auth_credentials_store".into(),toml::Value::String("file".into()));
            doc.insert("web_search".into(),toml::Value::String(if profile.network{"live"}else{"disabled"}.into()));
            let mut servers=toml::Table::new();for name in &profile.mcp{let s=&config.mcp[name];let mut t=toml::Table::new();t.insert("command".into(),toml::Value::String(s.command.to_string_lossy().into_owned()));t.insert("args".into(),toml::Value::Array(s.args.iter().cloned().map(toml::Value::String).collect()));if !s.env_from.is_empty(){t.insert("env_vars".into(),toml::Value::Array(s.env_from.iter().cloned().map(toml::Value::String).collect()));}if s.approval_mode!="auto"{t.insert("default_tools_approval_mode".into(),toml::Value::String(s.approval_mode.clone()));}servers.insert(name.clone(),toml::Value::Table(t));}
            doc.insert("mcp_servers".into(),toml::Value::Table(servers));
            let mut projects=toml::Table::new();let mut receipt=toml::Table::new();receipt.insert("trust_level".into(),toml::Value::String("trusted".into()));projects.insert(request.workdir.to_string_lossy().into_owned(),toml::Value::Table(receipt));doc.insert("projects".into(),toml::Value::Table(projects));
            super::plugins::codex_config(&mut doc,&hooks,home,&plugins)?;
            super::codex::render_permissions(&mut doc,runtime,home)?;
            let text=toml::to_string_pretty(&doc).map_err(|_|invalid("native config could not be serialized"))?;p.file("config.toml",text.as_bytes(),0o600)?;
            let (source,target)=if let Some(a)=request.account.as_deref(){(account_home(app_home,kind,a).join("auth.json"),"auth.json".into())}
                else if let Some(Auth::FileLink{source,target})=&runtime.auth{(source.clone(),target.clone())}
                else{let global=std::env::var_os("CODEX_HOME").map(PathBuf::from).unwrap_or_else(||PathBuf::from(std::env::var_os("HOME").unwrap_or_default()).join(".codex"));(global.join("auth.json"),"auth.json".into())};
            p.link(&target,&source)?;
        },
        Adapter::Claude|Adapter::Glm=>{
            let mut settings=native;settings["hooks"]=hooks.clone();
            p.json("settings.json",&settings)?;p.json("mcp/mcp-config.json",&json!({"mcpServers":mcp}))?;
            if let Some(Auth::FileLink{source,target})=&runtime.auth{p.link(target,source)?;}
        },
        Adapter::Qwen=>{
            let mut settings=native;settings["context"]=json!({"fileName":home.join("agent-run-context.md")});settings["tools"]=json!({"sandbox":true});settings["security"]=json!({"auth":{"selectedType":"openai"}});settings["mcpServers"]=Value::Object(mcp);
            let mut qhooks=plugins.qwen_hooks.clone();if let (Some(target),Some(extra))=(qhooks.as_object_mut(),hooks.as_object()){for(k,v)in extra{target.entry(k.clone()).or_insert(json!([])).as_array_mut().ok_or_else(||invalid("invalid Qwen hook group"))?.extend(v.as_array().cloned().unwrap_or_default());}}
            settings["hooks"]=qhooks;
            let mut context=profile.body.clone();for skill in &profile.skills{context.push_str(&format!("\n\nDeclared skill: {}\n",home.join("skills").join(skill).join("SKILL.md").display()));}
            p.file("agent-run-context.md",context.as_bytes(),0o600)?;p.json(".qwen/settings.json",&settings)?;
        }
    }
    if let Some(env)=runtime.environment.as_ref().and_then(|n|config.environments.get(n)){
        let exe=std::env::current_exe()?;let mut rules=String::new();
        for name in &env.denied_commands{
            let wrapper=format!("#!/bin/sh\nexec {} _deny-command {}\n",shell_quote(&exe.to_string_lossy()),shell_quote(name));
            p.file(&format!("command-refusals/{name}"),wrapper.as_bytes(),0o700)?;
            let host_path=std::env::var("PATH").unwrap_or_default();let mut commands=vec![name.clone()];if let Some(path)=resolve_executable(name,&host_path){commands.push(path.to_string_lossy().into_owned());}
            for command in commands{rules.push_str(&format!("prefix_rule(pattern = [{}], decision = \"forbidden\")\n",serde_json::to_string(&command)?));}
        }
        if kind==Adapter::Codex{p.file("rules/agent-run-command-policy.rules",rules.as_bytes(),0o600)?;}
    }
    p.finish()
}
