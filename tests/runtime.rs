//! Runtime unit and integration tests independent of generated endpoint shape.

use archastro::sse::SseDecode;
use archastro::{AppSession, Client, Error, SessionStore};
use futures_util::StreamExt;
use httpmock::prelude::*;
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug, Serialize)]
struct Query<'a> {
    search: &'a str,
    page: i64,
}

#[derive(Default)]
struct MemorySessionStore(std::sync::Mutex<Option<AppSession>>);

#[async_trait::async_trait]
impl SessionStore for MemorySessionStore {
    async fn load(
        &self,
    ) -> std::result::Result<Option<AppSession>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.0.lock().unwrap().clone())
    }

    async fn save(
        &self,
        session: &AppSession,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        *self.0.lock().unwrap() = Some(session.clone());
        Ok(())
    }

    async fn clear(&self) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        *self.0.lock().unwrap() = None;
        Ok(())
    }
}

#[tokio::test]
async fn http_serializes_auth_query_and_body() {
    let server = MockServer::start_async().await;
    let request = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/api/v1/widgets/a%2Fb")
                .query_param("search", "two words")
                .query_param("page", "2")
                .header("x-archastro-api-key", "sk_test")
                .header("authorization", "Bearer token")
                .json_body(json!({ "enabled": true }));
            then.status(200).json_body(json!({ "id": "wid_1" }));
        })
        .await;
    let client = Client::builder()
        .base_url(server.base_url())
        .secret_key("sk_test")
        .access_token("token")
        .build()
        .unwrap();
    let result: Value = client
        .request(
            Method::POST,
            &format!("/api/v1/widgets/{}", archastro::encode_path("a/b")),
        )
        .query(&Query {
            search: "two words",
            page: 2,
        })
        .unwrap()
        .json(&json!({ "enabled": true }))
        .unwrap()
        .send()
        .await
        .unwrap();
    assert_eq!(result["id"], "wid_1");
    request.assert_calls_async(1).await;
}

#[tokio::test]
async fn api_errors_preserve_status_code_message_and_body() {
    let server = MockServer::start_async().await;
    server.mock_async(|when, then| {
        when.method(GET).path("/failure");
        then.status(422).json_body(json!({ "error": { "code": "invalid_widget", "message": "bad widget" }, "trace": "abc" }));
    }).await;
    let client = Client::builder()
        .base_url(server.base_url())
        .build()
        .unwrap();
    let error = client
        .request(Method::GET, "/failure")
        .send::<Value>()
        .await
        .unwrap_err();
    match error {
        Error::Api(error) => {
            assert_eq!(error.status, 422);
            assert_eq!(error.code.as_deref(), Some("invalid_widget"));
            assert_eq!(error.message, "bad widget");
            assert_eq!(error.body["trace"], "abc");
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[tokio::test]
async fn concurrent_unauthorized_requests_share_one_single_use_refresh() {
    let server = MockServer::start_async().await;
    let unauthorized = server
        .mock_async(|when, then| {
            when.method(GET)
                .path("/protected")
                .header("authorization", "Bearer old");
            then.status(401).json_body(json!({ "code": "expired" }));
        })
        .await;
    let refresh = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/auth/refresh")
                .json_body(json!({ "refresh_token": "refresh-old" }));
            then.status(200)
                .json_body(json!({ "access_token": "new", "refresh_token": "refresh-new" }));
        })
        .await;
    let success = server
        .mock_async(|when, then| {
            when.method(GET)
                .path("/protected")
                .header("authorization", "Bearer new");
            then.status(200).json_body(json!({ "ok": true }));
        })
        .await;
    let client = Client::builder()
        .base_url(server.base_url())
        .access_token("old")
        .build()
        .unwrap();
    client
        .install_session("old".into(), Some("refresh-old".into()), "/auth/refresh")
        .await;

    let calls = (0..12).map(|_| {
        let client = client.clone();
        tokio::spawn(async move {
            client
                .request(Method::GET, "/protected")
                .send::<Value>()
                .await
                .unwrap()
        })
    });
    for call in calls {
        assert_eq!(call.await.unwrap(), json!({ "ok": true }));
    }

    assert!(unauthorized.calls_async().await >= 1);
    refresh.assert_calls_async(1).await;
    success.assert_calls_async(12).await;
    assert_eq!(client.access_token().await.as_deref(), Some("new"));
}

#[derive(Debug, PartialEq, Deserialize)]
struct Updated {
    id: String,
}

impl SseDecode for Updated {
    fn decode(event: &str, data: &str) -> archastro::Result<Self> {
        if event != "updated" {
            return Err(Error::UnknownSseEvent(event.into()));
        }
        Ok(serde_json::from_str(data)?)
    }
}

#[tokio::test]
async fn sse_parses_fragmented_contract_events() {
    let server = MockServer::start_async().await;
    server
        .mock_async(|when, then| {
            when.method(GET).path("/events");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body("id: 42\nevent: updated\ndata: {\"id\":\"one\"}\n\n");
        })
        .await;
    let client = Client::builder()
        .base_url(server.base_url())
        .build()
        .unwrap();
    let mut stream = client
        .request(Method::GET, "/events")
        .stream::<Updated>()
        .await
        .unwrap();
    let event = stream.next().await.unwrap().unwrap();
    assert_eq!(event.id, "42");
    assert_eq!(event.event, "updated");
    assert_eq!(event.data, Updated { id: "one".into() });
    stream.close();
}

#[tokio::test]
async fn sse_reconnects_after_a_flushed_transport_disconnect() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        for id in ["one", "two"] {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = vec![0_u8; 2048];
            let _ = socket.read(&mut request).await.unwrap();
            let body = format!("event: updated\ndata: {{\"id\":\"{id}\"}}\n\n");
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\n{body}"
            );
            socket.write_all(response.as_bytes()).await.unwrap();
            socket.flush().await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    });
    let client = Client::builder()
        .base_url(format!("http://{address}"))
        .build()
        .unwrap();
    let mut stream = client
        .request(Method::GET, "/events")
        .stream::<Updated>()
        .await
        .unwrap();
    assert_eq!(stream.next().await.unwrap().unwrap().data.id, "one");

    let mut saw_disconnect = false;
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            match stream.next().await {
                Some(Ok(event)) if saw_disconnect => {
                    assert_eq!(event.data.id, "two");
                    break;
                }
                Some(Err(Error::Sse(_))) => saw_disconnect = true,
                Some(_) => {}
                None => panic!("stream closed instead of reconnecting"),
            }
        }
    })
    .await
    .expect("stream reconnect deadline");
}

#[tokio::test]
async fn sse_open_uses_the_same_single_use_refresh_contract_as_rest() {
    let server = MockServer::start_async().await;
    let unauthorized = server
        .mock_async(|when, then| {
            when.method(GET)
                .path("/events")
                .header("authorization", "Bearer old");
            then.status(401)
                .json_body(json!({ "code": "expired", "message": "expired" }));
        })
        .await;
    let refresh = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/auth/refresh")
                .json_body(json!({ "refresh_token": "refresh-old" }));
            then.status(200)
                .json_body(json!({ "access_token": "new", "refresh_token": "refresh-new" }));
        })
        .await;
    let stream_response = server
        .mock_async(|when, then| {
            when.method(GET)
                .path("/events")
                .header("authorization", "Bearer new");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body("event: updated\ndata: {\"id\":\"refreshed\"}\n\n");
        })
        .await;
    let client = Client::builder()
        .base_url(server.base_url())
        .access_token("old")
        .build()
        .unwrap();
    client
        .install_session("old".into(), Some("refresh-old".into()), "/auth/refresh")
        .await;

    let mut stream = client
        .request(Method::GET, "/events")
        .stream::<Updated>()
        .await
        .unwrap();
    assert_eq!(stream.next().await.unwrap().unwrap().data.id, "refreshed");
    unauthorized.assert_calls_async(1).await;
    refresh.assert_calls_async(1).await;
    stream_response.assert_calls_async(1).await;
}

#[tokio::test]
async fn app_sessions_restore_persist_rotation_and_sign_out() {
    let server = MockServer::start_async().await;
    server
        .mock_async(|when, then| {
            when.method(GET)
                .path("/protected")
                .header("authorization", "Bearer old");
            then.status(401).json_body(json!({ "code": "expired" }));
        })
        .await;
    let refresh = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/api/v1/auth/refresh")
                .json_body(json!({ "refresh_token": "refresh-old" }));
            then.status(200)
                .json_body(json!({ "access_token": "new", "refresh_token": "refresh-new" }));
        })
        .await;
    server
        .mock_async(|when, then| {
            when.method(GET)
                .path("/protected")
                .header("authorization", "Bearer new");
            then.status(200).json_body(json!({ "ok": true }));
        })
        .await;
    let store = std::sync::Arc::new(MemorySessionStore::default());
    *store.0.lock().unwrap() = Some(AppSession {
        access_token: "old".into(),
        refresh_token: Some("refresh-old".into()),
        access_token_expires_at: Some(123),
        user: Some(json!({ "id": "usr_1" })),
    });
    let client = Client::builder()
        .base_url(server.base_url())
        .session_store(store.clone())
        .build()
        .unwrap();
    client.restore_session().await.unwrap().unwrap();
    let response: Value = client
        .request(Method::GET, "/protected")
        .send()
        .await
        .unwrap();
    assert_eq!(response, json!({ "ok": true }));
    refresh.assert_calls_async(1).await;
    let persisted = store.0.lock().unwrap().clone().unwrap();
    assert_eq!(persisted.access_token, "new");
    assert_eq!(persisted.refresh_token.as_deref(), Some("refresh-new"));
    assert_eq!(persisted.user, Some(json!({ "id": "usr_1" })));

    client.sign_out().await.unwrap();
    assert!(store.0.lock().unwrap().is_none());
    assert!(client.access_token().await.is_none());
}

#[test]
#[cfg(feature = "blocking")]
fn blocking_bridge_executes_outside_a_runtime() {
    let result = archastro::blocking::block_on(async { Ok::<_, Error>(42) }).unwrap();
    assert_eq!(result, 42);
}
