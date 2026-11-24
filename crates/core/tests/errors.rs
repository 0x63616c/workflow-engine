use wfe_core::{Error, ErrorCode};

#[test]
fn require_fields_names_every_empty_field_and_the_full_list() {
    let error =
        Error::require_fields("StartWorkflow", &[("workflow_id", ""), ("workflow_type", "Order"), ("task_queue", "")])
            .expect_err("two required fields are empty");
    assert_eq!(error.code, ErrorCode::InvalidArgument);
    assert_eq!(
        error.message,
        "StartWorkflow has empty workflow_id, task_queue: set every required field (workflow_id, workflow_type, task_queue)"
    );
}

#[test]
fn require_fields_accepts_a_complete_request() {
    assert_eq!(Error::require_fields("RegisterWorker", &[("build_id", "v1")]), Ok(()));
}
