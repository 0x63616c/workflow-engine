use strum::VariantArray;
use wfe_core::ErrorCode;
use wfe_core::api::WorkflowStatus;

#[test]
fn every_error_code_parses_back_from_its_name_and_matches_the_wire_name() {
    for &code in ErrorCode::VARIANTS {
        assert_eq!(code.as_str().parse::<ErrorCode>(), Ok(code), "{code:?} does not round-trip through as_str");
        assert_eq!(
            serde_json::to_value(code).expect("an ErrorCode always serializes"),
            serde_json::Value::from(code.as_str()),
            "{code:?}: the JSON wire name and as_str() disagree; keep serde and strum on the same rename_all",
        );
    }
}

#[test]
fn every_workflow_status_parses_back_from_its_name() {
    for &status in WorkflowStatus::VARIANTS {
        assert_eq!(WorkflowStatus::parse(status.as_str()), Some(status), "{status:?} does not round-trip");
    }
    assert_eq!(WorkflowStatus::TimedOut.as_str(), "timed_out");
    assert_eq!(WorkflowStatus::parse("bogus"), None);
}

#[test]
fn workflow_status_keeps_its_stored_pascal_case_encoding() {
    assert_eq!(
        serde_json::to_value(WorkflowStatus::ContinuedAsNew).expect("a WorkflowStatus always serializes"),
        serde_json::Value::from("ContinuedAsNew"),
        "WorkflowStatus is stored in histories: changing its serde names breaks every existing data directory",
    );
}
