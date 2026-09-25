//! Golden and metadata contracts for the one public tool registry.

use agent_run_domain::{registry, tool, tools_json, ArgumentDefault};
use serde_json::Value;

/// Parses the captured Python discovery payload shared by all registry assertions.
fn golden() -> Vec<Value> {
    serde_json::from_str(include_str!("../../../tests/fixtures/baseline/tools.json"))
        .expect("Python tools golden must remain valid JSON")
}

/// Pin every public discovery field and registry ordering to the Python release.
///
/// The additive `delegation_guide` read has no entry in the frozen Python
/// baseline, so it is excluded here and pinned separately by
/// [`delegation_guide_is_the_additive_twelfth_tool`] — the historical fixture
/// itself is never edited to conceal the expected difference.
#[test]
fn registry_matches_python_golden_field_by_field() {
    let expected = golden();
    let actual: Vec<Value> = tools_json()
        .into_iter()
        .filter(|tool| tool["name"] != "delegation_guide")
        .collect();
    assert_eq!(tools_json().len(), 12);
    assert_eq!(actual.len(), expected.len());

    for (definition, (actual, mut expected)) in registry()
        .iter()
        .filter(|definition| definition.name != "delegation_guide")
        .zip(actual.iter().zip(expected))
    {
        assert_eq!(actual["name"], expected["name"]);
        // The schema-2 catalog reads extend the baseline: their descriptions
        // keep the Python text as a prefix, and they add only the exact
        // optional nullable string filters pinned here.
        let filters: &[&str] = match definition.name.as_str() {
            "models" => &["model", "profile", "provider"],
            "capacity_order" => &["model"],
            _ => &[],
        };
        if !filters.is_empty() {
            let base = expected["description"].as_str().unwrap();
            assert!(actual["description"].as_str().unwrap().starts_with(base));
            expected["description"] = actual["description"].clone();
            for name in filters {
                expected["inputSchema"]["properties"][name] =
                    serde_json::json!({"type": ["string", "null"]});
            }
        }
        // Schema-2 cutover: public start names a configured provider instead of
        // a legacy runtime; nothing else in its input schema changes.
        if definition.name == "start" {
            rename_runtime_to_provider(&mut expected);
        }
        if definition.name != "start" {
            assert_eq!(actual["description"], expected["description"]);
        }
        // The start description is pinned by `start_description_extends_the_python_baseline_exactly`.
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

/// The only text the start description adds to the frozen Python baseline: the
/// binding guidance, inserted directly after the baseline's opening sentence. It names both direct host-visible aliases and forbids indirect calls.
const START_BINDING_GUIDANCE: &str = "Automatic PostToolUse hook binding requires a direct, host-visible mcp__agent_run__start or mcp__agent-run__start call; do not wrap or nest start inside functions.exec, a shell call, another tool, or any other indirect invocation when automatic binding is expected. If a direct call is unavailable, pass the current session identity in orchestrator; otherwise delivery remains bound:false and no completion notice will arrive automatically. ";

/// Pins the start description to the Python baseline plus exactly the binding
/// guidance: removing that one insertion must reproduce the baseline byte for byte,
/// so no historical guidance can disappear or drift unnoticed.
#[test]
fn start_description_extends_the_python_baseline_exactly() {
    let baseline = golden()
        .into_iter()
        .find(|tool| tool["name"] == "start")
        .expect("golden start tool")["description"]
        .as_str()
        .expect("golden description is a string")
        .to_owned();
    let description = &tool("start").expect("start tool").description;
    assert_eq!(description.matches(START_BINDING_GUIDANCE).count(), 1);
    assert_eq!(
        description.replacen(START_BINDING_GUIDANCE, "", 1),
        baseline
    );
}

/// The additive `delegation_guide` read extends the frozen Python table by
/// exactly one no-argument, strict-object tool with its own error classes;
/// no baseline entry exists for it and none is invented.
#[test]
fn delegation_guide_is_the_additive_twelfth_tool() {
    let definition = tool("delegation_guide").expect("delegation_guide tool");
    assert!(golden()
        .iter()
        .all(|tool| tool["name"] != "delegation_guide"));
    assert!(definition.arguments().is_empty());
    assert_eq!(definition.input_schema["additionalProperties"], false);
    assert_eq!(definition.input_schema["properties"], serde_json::json!({}));
    assert!(!definition.error_classes().is_empty());
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

/// Applies the one deliberate start-schema change over the Python baseline:
/// the required `runtime` property becomes the required `provider`.
fn rename_runtime_to_provider(tool: &mut Value) {
    let schema = &mut tool["inputSchema"];
    let runtime = schema["properties"]
        .as_object_mut()
        .unwrap()
        .remove("runtime")
        .expect("baseline start declares runtime");
    schema["properties"]["provider"] = runtime;
    for name in schema["required"].as_array_mut().unwrap() {
        if name == "runtime" {
            *name = Value::from("provider");
        }
    }
}
