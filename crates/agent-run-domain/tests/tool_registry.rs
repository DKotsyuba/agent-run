//! Golden and metadata contracts for the one public tool registry.

use agent_run_domain::{registry, tools_json, ArgumentDefault};
use serde_json::Value;

/// Parses the captured Python discovery payload shared by all registry assertions.
fn golden() -> Vec<Value> {
    serde_json::from_str(include_str!("../../../tests/fixtures/baseline/tools.json"))
        .expect("Python tools golden must remain valid JSON")
}

/// Pin every public discovery field and registry ordering to the Python release.
#[test]
fn registry_matches_python_golden_field_by_field() {
    let expected = golden();
    let actual = tools_json();
    assert_eq!(actual.len(), 11);
    assert_eq!(actual.len(), expected.len());

    for (definition, (actual, expected)) in registry().iter().zip(actual.iter().zip(expected)) {
        assert_eq!(actual["name"], expected["name"]);
        assert_eq!(actual["description"], expected["description"]);
        assert_eq!(actual["inputSchema"], expected["inputSchema"]);
        assert_eq!(actual["outputSchema"], expected["outputSchema"]);
        assert_eq!(actual["resultShape"], expected["resultShape"]);
        assert!(!definition.error_classes().is_empty());

        let expected_properties = expected["inputSchema"]["properties"]
            .as_object()
            .expect("golden input properties are an object");
        let arguments = definition.arguments();
        assert_eq!(arguments.len(), expected_properties.len());
        for argument in arguments {
            assert_eq!(argument.schema, &expected_properties[argument.name]);
            assert_eq!(
                argument.required,
                expected["inputSchema"]["required"]
                    .as_array()
                    .is_some_and(|required| {
                        required
                            .iter()
                            .any(|name| name.as_str() == Some(argument.name))
                    })
            );
        }
    }
}

/// Keep the checked-in asset byte-identical to the captured Python golden.
#[test]
fn checked_in_asset_is_the_verified_python_golden() {
    assert_eq!(
        include_str!("../../../assets/tools.json"),
        include_str!("../../../tests/fixtures/baseline/tools.json")
    );
}

/// Preserve dispatch defaults that JSON Schema deliberately does not encode.
#[test]
fn registry_exposes_python_optional_argument_defaults() {
    let start = registry().iter().find(|tool| tool.name == "start").unwrap();
    assert_eq!(
        start
            .arguments()
            .into_iter()
            .find(|argument| argument.name == "read_roots")
            .unwrap()
            .default,
        Some(ArgumentDefault::EmptyArray)
    );
    let agents = registry()
        .iter()
        .find(|tool| tool.name == "list_agents")
        .unwrap();
    assert_eq!(
        agents
            .arguments()
            .into_iter()
            .find(|argument| argument.name == "limit")
            .unwrap()
            .default,
        Some(ArgumentDefault::Integer(100))
    );
}
