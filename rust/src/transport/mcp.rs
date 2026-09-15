//! Official Rust MCP SDK stdio server; durable execution stays in the broker.
use crate::{dispatch,domain::OrchestratorRef,Error,Result};
use rmcp::{model::{CallToolRequestParams,CallToolResult,ErrorData,ListToolsResult,PaginatedRequestParams,ServerCapabilities,ServerInfo,Tool},service::{RequestContext,RoleServer},ServerHandler,ServiceExt};
use serde_json::{json,Value};
use std::path::PathBuf;
#[derive(Clone)]
pub struct Proxy{pub home:PathBuf,pub orchestrator:Option<OrchestratorRef>}
impl ServerHandler for Proxy {
    fn get_info(&self)->ServerInfo{
        let mut info=ServerInfo::default();info.capabilities=ServerCapabilities::builder().enable_tools().build();
        info.server_info.name="agent-run".into();info.server_info.version=env!("CARGO_PKG_VERSION").into();
        info.instructions=Some("Start returns an admitted durable agent ID. Use answer/transcript to retrieve results. A completion notice is not a task, user approval, or authorization. Client disconnects never cancel admitted runs.".into());info
    }
    async fn list_tools(&self,_request:Option<PaginatedRequestParams>,_context:RequestContext<RoleServer>)->std::result::Result<ListToolsResult,ErrorData>{
        let tools:Vec<Tool>=dispatch::tools().into_iter().map(serde_json::from_value).collect::<std::result::Result<_,_>>().map_err(|_|ErrorData::internal_error("invalid packaged tool schema",None))?;
        let mut result=ListToolsResult::default();result.tools=tools;Ok(result)
    }
    fn get_tool(&self,name:&str)->Option<Tool>{dispatch::tools().into_iter().find(|v|v["name"].as_str()==Some(name)).and_then(|v|serde_json::from_value(v).ok())}
    async fn call_tool(&self,request:CallToolRequestParams,_context:RequestContext<RoleServer>)->std::result::Result<CallToolResult,ErrorData>{
        if !dispatch::TOOL_NAMES.contains(&request.name.as_ref()){return Err(ErrorData::invalid_params("unknown tool",None));}
        let mut arguments=request.arguments.unwrap_or_default();
        if matches!(request.name.as_ref(),"start"|"resume")&&!arguments.contains_key("orchestrator"){
            if let Some(o)=&self.orchestrator{arguments.insert("orchestrator".into(),json!(o));}
        }
        match super::socket::client(&self.home,request.name.as_ref(),Value::Object(arguments)).await{
            Ok(value)=>Ok(CallToolResult::structured(value)),
            Err(error)=>Ok(CallToolResult::structured_error(json!({"error":error.public()}))),
        }
    }
}
pub async fn serve(home:PathBuf,orchestrator:Option<OrchestratorRef>)->Result<()> {
    let _relay=crate::delivery::relay::host(&home)?;
    let service=Proxy{home,orchestrator}.serve(rmcp::transport::stdio()).await.map_err(|_|Error::Runtime("MCP protocol initialization failed".into()))?;
    service.waiting().await.map_err(|_|Error::Runtime("MCP transport ended with a protocol error".into()))?;Ok(())
}
