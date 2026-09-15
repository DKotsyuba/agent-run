//! Offline fake engine. It neither contacts a provider nor executes task text.
//! Only explicit fixture-mode keywords change deterministic test behavior.
use serde_json::{json,Value};
use std::io::{self,BufRead,Write};
use std::time::Duration;
fn emit(value:Value){println!("{value}");io::stdout().flush().expect("fixture stdout");}
fn argument(args:&[String],name:&str)->Option<String>{args.iter().position(|s|s==name).and_then(|i|args.get(i+1)).cloned()}
fn main(){
    let args:Vec<String>=std::env::args().skip(1).collect();
    if args.iter().any(|s|s=="--version"){println!("agent-run offline fixture 1.0");return;}
    let session=argument(&args,"--resume").or_else(||argument(&args,"--session-id")).unwrap_or_else(||"fixture-session".into());
    let task=if let Some(task)=argument(&args,"-p"){task}else{
        let mut line=String::new();io::stdin().lock().read_line(&mut line).expect("fixture input");
        let value:Value=serde_json::from_str(&line).expect("fixture JSON");
        value.pointer("/message/content/0/text").and_then(Value::as_str).unwrap_or("").to_owned()
    };
    emit(json!({"type":"system","subtype":"init","session_id":session}));
    emit(json!({"type":"assistant","session_id":session,"message":{"content":[{"type":"text","text":"fixture partial\n"}]}}));
    if task=="fixture:hang"{loop{std::thread::sleep(Duration::from_secs(1));}}
    if task=="fixture:missing-result"{return;}
    if task=="fixture:slow"{std::thread::sleep(Duration::from_secs(3));}
    let failed=task=="fixture:error";
    emit(json!({"type":"result","subtype":if failed{"error_during_execution"}else{"success"},"is_error":failed,"session_id":session,"result":if failed{"fixture failure"}else{"fixture final answer\n"},"usage":{"input_tokens":2,"output_tokens":3},"num_turns":1}));
}
