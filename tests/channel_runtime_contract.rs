//! Fault-injection contracts for the hand-maintained Phoenix runtime.

mod support;

use std::time::Duration;

use archastro::generated::{ApiObjectChannel, ApiObjectChannelJoinByIdParams};
use archastro::{ChannelState, Error, SocketBuilder, SocketEvent};
use futures_util::StreamExt;
use serde_json::json;

fn join_params() -> ApiObjectChannelJoinByIdParams {
    serde_json::from_value(json!({})).expect("valid join params")
}

#[tokio::test]
#[serial_test::serial]
#[ignore = "requires channel harness"]
async fn join_rejection_preserves_the_server_payload() {
    support::mark_all_used();
    let harness = support::harness().await;
    harness
        .register_scenario(&json!({
            "topic": "api:object:test-id",
            "onJoin": [{ "type": "replyError", "payload": { "reason": "denied" } }]
        }))
        .await;
    let socket = harness.socket().await;
    let error = match ApiObjectChannel::join_by_id(&socket, "test-id", &join_params()).await {
        Ok(_) => panic!("join must fail"),
        Err(error) => error,
    };
    match error {
        Error::Channel(error) => assert_eq!(error.payload, Some(json!({ "reason": "denied" }))),
        other => panic!("expected channel error, got {other:?}"),
    }
}

#[tokio::test]
#[serial_test::serial]
#[ignore = "requires channel harness"]
async fn push_rejection_and_timeout_are_typed_errors() {
    let harness = support::harness().await;
    harness
        .register_scenario(&json!({
            "topic": "api:object:test-id",
            "onJoin": [{ "type": "autoReply" }],
            "onMessage": {
                "save": [{ "type": "replyError", "payload": { "reason": "conflict" } }],
                "presence_update": [{ "type": "replyTimeout" }]
            }
        }))
        .await;
    let socket = SocketBuilder::new(harness.ws_url())
        .timeout(Duration::from_millis(50))
        .connect()
        .await
        .expect("connect socket");
    let channel = ApiObjectChannel::join_by_id(&socket, "test-id", &join_params())
        .await
        .expect("join channel");

    assert!(matches!(channel.save().await, Err(Error::Channel(_))));
    let timeout = channel
        .channel
        .push("presence_update", json!({ "presence": {} }))
        .await;
    assert!(matches!(timeout, Err(Error::Timeout)));
}

#[tokio::test]
#[serial_test::serial]
#[ignore = "requires channel harness"]
async fn disconnect_reconnects_rejoins_and_flushes_buffered_pushes() {
    let harness = support::harness().await;
    harness
        .register_scenario(&json!({
            "topic": "api:object:test-id",
            "onJoin": [{ "type": "autoReply" }],
            "onMessage": {
                "update_fields": [{ "type": "disconnect" }],
                "save": [{ "type": "autoReply" }]
            }
        }))
        .await;
    let socket = SocketBuilder::new(harness.ws_url())
        .reconnect_backoff([Duration::from_millis(150)])
        .connect()
        .await
        .expect("connect socket");
    let mut socket_events = socket.events();
    let channel = ApiObjectChannel::join_by_id(&socket, "test-id", &join_params())
        .await
        .expect("join channel");

    let disconnect = channel
        .channel
        .push("update_fields", json!({ "fields": {} }))
        .await;
    assert!(matches!(disconnect, Err(Error::Closed)));
    assert!(matches!(
        socket_events.next().await,
        Some(SocketEvent::Close { .. })
    ));

    let save = channel.save();
    tokio::pin!(save);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut save)
            .await
            .is_err()
    );
    assert_eq!(socket_events.next().await, Some(SocketEvent::Open));
    save.await.expect("buffered push flushes after rejoin");
    assert_eq!(channel.channel.state().await, ChannelState::Joined);
}

#[tokio::test]
#[serial_test::serial]
#[ignore = "requires channel harness"]
async fn acknowledged_heartbeats_keep_the_channel_usable() {
    let harness = support::harness().await;
    harness
        .register_scenario(&json!({
            "topic": "api:object:test-id",
            "onJoin": [{ "type": "autoReply" }],
            "onMessage": { "save": [{ "type": "autoReply" }] }
        }))
        .await;
    let socket = SocketBuilder::new(harness.ws_url())
        .heartbeat(Duration::from_millis(20))
        .connect()
        .await
        .expect("connect socket");
    let channel = ApiObjectChannel::join_by_id(&socket, "test-id", &join_params())
        .await
        .expect("join channel");
    tokio::time::sleep(Duration::from_millis(75)).await;
    channel.save().await.expect("push after heartbeats");
    assert!(socket.is_connected());
}
