//! Strict exact-export request and response wire contracts; no database involved.
use agent_run_domain::transcript_exact::{
    TranscriptExactRequest, TranscriptExactResponse, sequence,
};
use serde_json::{Value, json};

/// Supplies a syntactically valid fence without asserting stored evidence exists.
fn request() -> Value {
    json!({"version":1,"expected_store_id":"store_00000000000000000000000000000000","selector":{"agent_id":"ag-20260826-120000-0123456789","sequence":1,"execution_ref":format!("ex1_{}","0".repeat(64)),"pin_id":"pin_00000000000040008000000000000000"},"cursor":"0","offset":0})
}

/// Numeric cursors, ambiguous decimals, partial identities and null options reject.
#[test]
fn transcript_exact_strict_request_and_decimal_boundaries() {
    assert_eq!(sequence("9223372036854775807").unwrap(), i64::MAX);
    assert_eq!(sequence("9007199254740993").unwrap(), 9_007_199_254_740_993);
    for bad in [
        "",
        "00",
        "01",
        "+1",
        "-1",
        "1.0",
        " 1",
        "9223372036854775808",
    ] {
        assert!(sequence(bad).is_err());
    }
    let tool = agent_run_domain::tool("transcript_exact").unwrap();
    let schema = jsonschema::validator_for(&tool.input_schema).unwrap();
    let good = request();
    assert!(schema.is_valid(&good));
    assert!(
        serde_json::from_value::<TranscriptExactRequest>(good.clone())
            .unwrap()
            .validate()
            .is_ok()
    );
    for (key, value) in [
        ("cursor", json!(0)),
        ("upper_seq", Value::Null),
        ("expected_sha256", Value::Null),
        ("surprise", json!(true)),
    ] {
        let mut bad = good.clone();
        bad[key] = value;
        assert!(!schema.is_valid(&bad));
        assert!(serde_json::from_value::<TranscriptExactRequest>(bad).is_err());
    }
    for key in ["agent_id", "sequence", "execution_ref", "pin_id"] {
        let mut bad = good.clone();
        bad["selector"].as_object_mut().unwrap().remove(key);
        assert!(serde_json::from_value::<TranscriptExactRequest>(bad).is_err());
    }
    for bad in [
        json!({"cursor":"1"}),
        json!({"offset":1}),
        json!({"expected_sha256":"0".repeat(64)}),
        json!({"cursor":"2","upper_seq":"1"}),
        json!({"cursor":"1","upper_seq":"1","offset":1,"expected_sha256":"0".repeat(64)}),
    ] {
        let mut raw = good.clone();
        raw.as_object_mut()
            .unwrap()
            .extend(bad.as_object().unwrap().clone());
        assert!(
            serde_json::from_value::<TranscriptExactRequest>(raw)
                .unwrap()
                .validate()
                .is_err()
        );
    }
    let duplicate = serde_json::to_string(&good).unwrap().replacen(
        "\"cursor\":\"0\"",
        "\"cursor\":\"0\",\"cursor\":\"1\"",
        1,
    );
    assert!(serde_json::from_str::<TranscriptExactRequest>(&duplicate).is_err());
}

/// Response correlation rejects unrelated fragments and premature completion.
#[test]
fn transcript_exact_output_schema_and_correlation() {
    let raw = request();
    let request: TranscriptExactRequest = serde_json::from_value(raw.clone()).unwrap();
    let result = json!({"version":1,"store_id":raw["expected_store_id"],"selector":raw["selector"],"cursor":"0","upper_seq":"9007199254740993","offset":0,"message":{"seq":"9007199254740993","sha256":"0".repeat(64),"total_bytes":10,"start":0,"end":5,"text":"hello","complete":false},"next_cursor":"0","next_offset":5,"snapshot_complete":false});
    let schema = jsonschema::validator_for(
        agent_run_domain::tool("transcript_exact")
            .unwrap()
            .output_schema
            .as_ref()
            .unwrap(),
    )
    .unwrap();
    assert!(schema.is_valid(&result));
    assert!(
        serde_json::from_value::<TranscriptExactResponse>(result.clone())
            .unwrap()
            .validate_for(&request)
            .is_ok()
    );
    for (key, val) in [
        ("snapshot_complete", json!(true)),
        ("next_cursor", json!("9007199254740993")),
        ("next_offset", json!(4)),
        ("cursor", json!("1")),
        ("message", Value::Null),
    ] {
        let mut bad = result.clone();
        bad[key] = val;
        assert!(
            serde_json::from_value::<TranscriptExactResponse>(bad)
                .unwrap()
                .validate_for(&request)
                .is_err()
        );
    }
}

/// Empty pages require an explicit null message; omission is a malformed response.
#[test]
fn transcript_exact_empty_page_requires_message_field() {
    let raw = request();
    let request: TranscriptExactRequest = serde_json::from_value(raw.clone()).unwrap();
    let mut result = json!({"version":1,"store_id":raw["expected_store_id"],
        "selector":raw["selector"],"cursor":"0","upper_seq":"0","offset":0,
        "message":null,"next_cursor":"0","next_offset":0,"snapshot_complete":true});
    let page: TranscriptExactResponse = serde_json::from_value(result.clone()).unwrap();
    page.validate_for(&request).unwrap();
    result.as_object_mut().unwrap().remove("message");
    assert!(serde_json::from_value::<TranscriptExactResponse>(result).is_err());
}
