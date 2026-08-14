//! Fault-injection contracts for the hand-maintained SSE runtime.

mod support;

use archastro::Error;
use serde_json::json;

const ROUTE: &str = "GET /api/v1/agent_sessions/{agent_session}/stream";

#[tokio::test]
#[serial_test::serial]
#[ignore = "requires channel harness"]
async fn pre_stream_http_failure_is_a_structured_api_error() {
    support::mark_all_used();
    let harness = support::harness().await;
    harness
        .register_stream_actions(
            ROUTE,
            &[json!({
                "type": "status",
                "code": 402,
                "body": { "error": { "code": "payment_required", "message": "upgrade" } }
            })],
        )
        .await;
    let error = match harness
        .client()
        .v1()
        .agent_sessions()
        .stream("test-value")
        .await
    {
        Ok(_) => panic!("stream open must fail"),
        Err(error) => error,
    };
    match error {
        Error::Api(error) => {
            assert_eq!(error.status, 402);
            assert_eq!(error.code.as_deref(), Some("payment_required"));
            assert_eq!(error.message, "upgrade");
        }
        other => panic!("expected API error, got {other:?}"),
    }
}
