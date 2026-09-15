//! Never upgrade tool filtering or home isolation into an OS sandbox guarantee.
use crate::{config::Runtime, error::invalid,profiles::Profile,Result};
use serde::{Serialize,Deserialize};
#[derive(Debug,Clone,Copy,PartialEq,Eq,PartialOrd,Ord,Serialize,Deserialize)]
#[serde(rename_all="snake_case")]
pub enum Constraint {WebToolsDisabled,ExternalNetworkIsolation,LoopbackTcpIsolation,UnixIpcIsolation,McpIpcIsolation,FilesystemWriteIsolation,FilesystemReadIsolation,PluginImmutability}
impl Constraint{
    pub const ALL:[Self;8]=[Self::WebToolsDisabled,Self::ExternalNetworkIsolation,Self::LoopbackTcpIsolation,Self::UnixIpcIsolation,Self::McpIpcIsolation,Self::FilesystemWriteIsolation,Self::FilesystemReadIsolation,Self::PluginImmutability];
}
#[derive(Debug,Clone,Serialize,Deserialize)]
#[serde(rename_all="snake_case")]
pub enum Enforcement {ToolFilter,RuntimeEnforced,OsEnforced,Advisory,Unsupported}
#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct Evidence{pub constraint:Constraint,pub enforcement:Enforcement,pub supported:bool,pub required:bool,pub scope:String,pub platform:String,pub reason:String}
#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct EffectivePolicy{pub runtime_name:String,pub platform:String,pub constraints:Vec<Evidence>}
pub fn evaluate(runtime_name:&str,runtime:&Runtime,profile:&Profile)->EffectivePolicy{
    let platform=if cfg!(target_os="macos"){"darwin"}else{"linux"};
    let constraints=Constraint::ALL.into_iter().map(|constraint|{
        let (enforcement,supported,reason)=match constraint{
            Constraint::WebToolsDisabled if !profile.network=>(Enforcement::ToolFilter,true,"native web tools are disabled; this is not network containment"),
            Constraint::PluginImmutability if runtime.plugins.is_empty()=>(Enforcement::RuntimeEnforced,true,"no configured plugin assets"),
            _=>(Enforcement::Unsupported,false,"no independently verified enforcement claim in this Rust port"),
        };
        Evidence{constraint,enforcement,supported,required:profile.required_constraints.contains(&constraint),scope:format!("{constraint:?}"),platform:platform.into(),reason:reason.into()}
    }).collect();
    EffectivePolicy{runtime_name:runtime_name.into(),platform:platform.into(),constraints}
}
impl EffectivePolicy{pub fn admit(&self)->Result<()>{if self.constraints.iter().any(|e|e.required&&!e.supported){return Err(invalid("required policy constraints are not enforced"));}Ok(())}}
