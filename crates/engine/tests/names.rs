use wfe_engine::workflow::{ActPhase, Wft};

#[test]
fn describe_shows_the_snake_case_variant_names() {
    assert_eq!(<&str>::from(ActPhase::Backoff), "backoff");
    assert_eq!(<&str>::from(&Wft::None), "none");
    assert_eq!(<&str>::from(&Wft::Scheduled { scheduled_id: 1, attempt: 1 }), "scheduled");
}
