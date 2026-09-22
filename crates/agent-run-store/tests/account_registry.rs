//! Disposable-store account registration and reference identity checks.

use agent_run_domain::{
    catalog::{
        AccountRecord, AccountStatus, AttemptCredentials, ProviderCatalog, ProviderDefinition,
    },
    AccountId, SecretRef,
};
use agent_run_store::Store;

/// Builds one fake metadata-only account record.
fn record(id: &str, family: &str, reference: &str) -> AccountRecord {
    AccountRecord {
        account_id: id.parse().unwrap(),
        auth_family: family.parse().unwrap(),
        secret_ref: reference.parse().unwrap(),
        status: AccountStatus::Enabled,
    }
}

/// Registration rejects raw tokens and duplicate storage identities; disable
/// changes status only and retains the account for historical resolution.
#[test]
fn registry_keeps_global_identity_and_reference_metadata() {
    let home = tempfile::tempdir().unwrap();
    Store::initialize(home.path()).unwrap();
    let mut store = Store::open(home.path()).unwrap();
    let account = record("acct-one", "openai", "native:codex");
    store.register_account(&account).unwrap();
    assert!(store.register_account(&account).is_err());
    assert!(store
        .register_account(&record("acct-two", "openai", "native:codex"))
        .is_err());
    assert!(store
        .register_account(&record("acct-two", "openai", "raw-fake-token"))
        .is_err());
    assert!(store
        .register_account(&record("acct-wrong", "anthropic", "native:codex"))
        .is_err());
    store
        .register_account(&record(
            "acct-two",
            "anthropic",
            "keychain:fake.service:FAKE_KEY",
        ))
        .unwrap();
    store
        .register_account(&record("acct-three", "openai", "named:codex:work"))
        .unwrap();
    store
        .register_account(&record("acct-four", "anthropic", "env:FAKE_TOKEN"))
        .unwrap();
    store
        .register_account(&record("acct-five", "anthropic", "file:/tmp/fake-auth"))
        .unwrap();
    assert_eq!(store.list_accounts().unwrap().len(), 5);
    let id: AccountId = "acct-one".parse().unwrap();
    store.conn.execute(
        "INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,timeout_seconds,config_revision) \
         VALUES ('ag-20260825-010203-0123456789','codex','gpt','role','task','task','/tmp','{}','running',1.0,10.0,'cfg')",
        [],
    ).unwrap();
    store.conn.execute(
        "INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,selected_account_id) \
         VALUES ('att-history','ag-20260825-010203-0123456789',1,'running','{}',1.0,'acct-one')",
        [],
    ).unwrap();
    store.disable_account(&id).unwrap();
    store.disable_account(&id).unwrap();
    let disabled = store.account(&id).unwrap().unwrap();
    assert_eq!(disabled.status, AccountStatus::Disabled);
    assert_eq!(
        disabled.secret_ref,
        "native:codex".parse::<SecretRef>().unwrap()
    );
    let selected: String = store
        .conn
        .query_row(
            "SELECT selected_account_id FROM attempts WHERE id='att-history'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(selected, id.as_str());
    let provider: ProviderDefinition = serde_json::from_value(serde_json::json!({
        "id":"codex","harness":"codex","connection":{"kind":"native"},
        "auth_family":"openai","limits_source":"codex_appserver",
        "models":[{"id":"gpt"}],
        "bindings":[{"label":"personal","account":"acct-one"}]
    }))
    .unwrap();
    let catalog = ProviderCatalog::new(vec![disabled], vec![provider]).unwrap();
    assert!(
        AttemptCredentials::from_selected(&catalog, &"codex".parse().unwrap(), "gpt", &id).is_err()
    );
    assert!(store
        .disable_account(&"acct-missing".parse().unwrap())
        .is_err());
    assert_eq!(store.list_accounts().unwrap().len(), 5);
}
