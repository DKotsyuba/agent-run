mod common;
use agent_run::capacity::{self,Key,Sample,Route,Pool,Topology};
use serde_json::json;
use std::collections::BTreeSet;
fn key()->Key{Key{runtime:"mock".into(),lane:"standard".into(),window:"five_hour".into(),target:None,source:"test".into()}}
fn sample(remaining:f64,observed:f64,reset:f64)->Sample{Sample{key:key(),remaining_percent:Some(remaining),reset_at:Some(reset),observed_at:Some(observed),valid_until:Some(observed+10000.0)}}
fn route(id:&str,account:Option<&str>)->Route{Route{route_id:id.into(),runtime:"mock".into(),account:account.map(str::to_owned),quota_lane:"standard".into(),pool_ids:vec!["pool".into()],reset_credits:None}}
fn response()->serde_json::Value{json!({"accountId":"ephemeral-do-not-persist","rateLimitResetCredits":{"availableCount":3},"rateLimitsByLimitId":{"codex":{"primary":{"usedPercent":25,"windowDurationMins":300,"resetsAt":20000},"secondary":{"usedPercent":10,"windowDurationMins":10080,"resetsAt":100000}}}})}
#[test]
fn nullable_account_identity_never_collides_with_a_label() {
    for label in ["base","default","shared","a:b","a/b","@base"]{assert_ne!(capacity::account_token(None),capacity::account_token(Some(label)));}
    assert_eq!(capacity::account_token(Some("a:b")),"@a%3Ab");
}
#[test]
fn codex_normalizer_keeps_windows_and_credit_metadata() {
    let(slice,account)=capacity::sources::normalize_codex("mock",Some("base"),&response(),1000.0).unwrap();
    assert_eq!(slice.samples.len(),2);assert_eq!(slice.topology.routes.len(),1);assert_eq!(slice.topology.routes[0].reset_credits,Some(3));
    assert_eq!(account.as_deref(),Some("ephemeral-do-not-persist"));assert!(!serde_json::to_string(&slice.topology).unwrap().contains("ephemeral-do-not-persist"));
}
#[test]
fn malformed_present_window_disables_the_whole_route() {
    let mut raw=response();raw["rateLimitsByLimitId"]["codex"]["secondary"]["usedPercent"]=json!(true);
    let(slice,_)=capacity::sources::normalize_codex("mock",None,&raw,1000.0).unwrap();
    assert_eq!(slice.samples.len(),1);assert!(slice.topology.routes.is_empty());
}
#[test]
fn codex_model_specific_bucket_does_not_inherit_standard_reset_credits() {
    let mut raw=response();raw["rateLimitsByLimitId"]["spark"]=json!({"limitName":"Spark","primary":{"usedPercent":0,"windowDurationMins":300,"resetsAt":20000}});
    let(slice,_)=capacity::sources::normalize_codex("mock",None,&raw,1000.0).unwrap();assert_eq!(slice.topology.routes.len(),2);
    let spark=slice.topology.routes.iter().find(|r|r.quota_lane=="Spark").unwrap();assert_eq!(spark.reset_credits,None);
}
#[test]
fn freshness_rejects_future_expired_reset_and_unknown_evidence() {
    let mut s=sample(80.0,1000.0,10000.0);assert!(s.fresh(1001.0));assert!(!s.fresh(999.0));assert!(!s.fresh(10000.0));
    s.valid_until=Some(1050.0);assert!(!s.fresh(1051.0));s.remaining_percent=None;assert!(!s.fresh(1001.0));
}
#[test]
fn reset_jitter_only_groups_still_open_windows() {
    let latest=sample(80.0,1000.0,2000.0);let near=sample(90.0,900.0,1999.5);assert!(capacity::same_cycle(&near,&latest));
    let rolled=sample(80.0,2000.0,2000.5);assert!(!capacity::same_cycle(&near,&rolled));
}
#[test]
fn burn_and_sustainable_rate_use_the_current_cycle() {
    let series=vec![sample(70.0,4600.0,11800.0),sample(90.0,1000.0,11800.0),sample(100.0,900.0,1000.0)];
    let forecast=capacity::forecast(&key(),&series,4600.0);assert_eq!(forecast.burn_percent_per_hour,Some(20.0));assert_eq!(forecast.burn_span_seconds,Some(3600.0));assert_eq!(forecast.sustainable_percent_per_hour,Some(35.0));
}
#[test]
fn thin_history_does_not_escalate_burn_risk() {
    let series=vec![sample(95.0,1010.0,20000.0),sample(100.0,1000.0,20000.0)];let f=capacity::forecast(&key(),&series,1010.0);
    assert_eq!(f.risk,"low");assert_eq!(capacity::window(&f,1010.0).unwrap().marker,"thin_evidence");
}
#[test]
fn exhausted_window_cannot_be_revived_by_weight() {
    let h=common::Home::new();let mut runtime=h.config.runtime("mock").unwrap().clone();runtime.priority_multiplier=1e100;
    let f=capacity::forecast(&key(),&[sample(0.0,1000.0,20000.0)],1000.0);let w=capacity::window(&f,1000.0).unwrap();assert!(capacity::rank(&runtime,vec![route("a",None)],vec![w]).is_none());
}
#[test]
fn aliases_use_highest_absolute_weight_not_sum() {
    let h=common::Home::new();let mut runtime=h.config.runtime("mock").unwrap().clone();runtime.priority_account_multipliers.insert("premium".into(),3.0);
    let f=capacity::forecast(&key(),&[sample(50.0,1000.0,20000.0)],1000.0);let w=capacity::window(&f,1000.0).unwrap();
    let r=capacity::rank(&runtime,vec![route("base",None),route("premium",Some("premium"))],vec![w]).unwrap();assert_eq!(r.multiplier,3.0);assert_eq!(r.priority,3.0);assert_eq!(r.aliases[0].account.as_deref(),Some("premium"));
}
#[test]
fn every_route_pool_reference_is_validated() {
    let mut topology=Topology{pools:vec![Pool{pool_id:"pool".into(),keys:BTreeSet::from([key()])}],routes:vec![route("a",None)]};assert!(topology.validate("mock").is_ok());
    topology.routes[0].pool_ids.push("missing".into());assert!(topology.validate("mock").is_err());
}
#[test]
fn invalid_atomic_slice_does_not_erase_a_committed_snapshot() {
    let h=common::Home::new();let(mut slice,_)=capacity::sources::normalize_codex("mock",None,&response(),agent_run::domain::now()).unwrap();
    capacity::persist(&h.path,&slice,100).unwrap();let before=h.store().health().unwrap();slice.samples[0].remaining_percent=Some(f64::NAN);assert!(capacity::persist(&h.path,&slice,100).is_err());assert_eq!(h.store().health().unwrap(),before);
    let db=rusqlite::Connection::open(h.path.join("state.db")).unwrap();let count:i64=db.query_row("SELECT COUNT(*) FROM capacity_samples",[],|r|r.get(0)).unwrap();assert_eq!(count,2);
}
#[test]
fn native_claude_requires_valid_percent_in_every_entry() {
    let good=json!({"limits":[{"kind":"session","percent":20,"resets_at":"2026-09-16T00:00:00Z"}]});assert_eq!(capacity::sources::normalize_claude("mock",&good,1000.0).unwrap().samples.len(),1);
    let bad=json!({"limits":[{"kind":"session","percent":"20"}]});assert!(capacity::sources::normalize_claude("mock",&bad,1000.0).is_err());
}
#[test]
fn codexbar_observation_time_must_have_a_timezone() {
    let good=json!({"usage":{"updatedAt":"2026-09-15T12:00:00Z","primary":{"usedPercent":25,"windowMinutes":300,"resetsAt":"2026-09-15T16:00:00Z"}}});
    assert!(capacity::sources::normalize_codexbar("mock",&good).is_ok());let mut bad=good;bad["usage"]["updatedAt"]=json!("2026-09-15T12:00:00");assert!(capacity::sources::normalize_codexbar("mock",&bad).is_err());
}
