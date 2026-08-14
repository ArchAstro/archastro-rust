use std::io::BufRead;
use std::process::{Child, Command, Stdio};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use archastro::{Client, Error};
use serde::Deserialize;
use serde_json::json;

static PRISM: OnceLock<Mutex<Child>> = OnceLock::new();
static HARNESS: OnceLock<(HarnessEndpoints, Mutex<Child>)> = OnceLock::new();

pub fn mark_all_used() {
    let _ = rest_client;
    let _ = assert_api_error;
    let _ = ensure_prism;
    let _ = prism_port;
    let _ = prism_url;
    let _ = wait_for_port;
    let _ = harness;
    let _ = Harness::client;
    let _ = Harness::socket;
    let _ = Harness::register_stream;
    let _ = Harness::register_stream_actions;
    let _ = Harness::register_channel;
    let _ = Harness::register_scenario;
    let _ = Harness::ws_url;
}

pub async fn rest_client(prefer: Option<u16>) -> Client {
    ensure_prism();
    let mut builder = Client::builder()
        .base_url(prism_url())
        .publishable_key("pk_test-key")
        .access_token("test-token");
    if let Some(status) = prefer {
        builder = builder.header("Prefer", format!("code={status}"));
    }
    builder.build().expect("contract client")
}

pub fn assert_api_error(error: Error, status: u16) {
    match error {
        Error::Api(error) => assert_eq!(error.status, status),
        other => panic!("expected API error {status}, got {other:?}"),
    }
}

fn ensure_prism() {
    PRISM.get_or_init(|| {
        let root = env!("CARGO_MANIFEST_DIR");
        let bin = std::env::var("PRISM_BIN")
            .unwrap_or_else(|_| format!("{root}/node_modules/.bin/prism"));
        let spec = std::env::var("OPENAPI_SPEC_PATH")
            .unwrap_or_else(|_| format!("{root}/specs/platform-openapi.json"));
        let child = Command::new(bin)
            .args(["mock", &spec, "--port", prism_port(), "--host", "127.0.0.1"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("start Prism");
        wait_for_port(prism_port());
        Mutex::new(child)
    });
}

fn prism_port() -> &'static str {
    option_env!("PRISM_PORT").unwrap_or("4040")
}
fn prism_url() -> String {
    format!("http://127.0.0.1:{}", prism_port())
}

fn wait_for_port(port: &str) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if std::net::TcpStream::connect(format!("127.0.0.1:{port}")).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("service did not listen on port {port}");
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HarnessEndpoints {
    ws_url: String,
    control_url: String,
}

pub struct Harness {
    endpoints: HarnessEndpoints,
    http: reqwest::Client,
}

pub async fn harness() -> Harness {
    let (endpoints, _) = HARNESS.get_or_init(|| {
        let root = env!("CARGO_MANIFEST_DIR");
        let bin = std::env::var("ARCHASTRO_HARNESS_BIN").unwrap_or_else(|_| {
            format!("{root}/node_modules/@archastro/channel-harness/dist/bin.js")
        });
        let spec = std::env::var("OPENAPI_SPEC_PATH")
            .unwrap_or_else(|_| format!("{root}/specs/platform-openapi.json"));
        let mut child = Command::new("node")
            .args([&bin, &spec])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("start channel harness");
        let line = std::io::BufReader::new(child.stdout.take().expect("harness stdout"))
            .lines()
            .next()
            .expect("harness URL line")
            .expect("read harness URL line");
        let endpoints = serde_json::from_str(&line).expect("parse harness URLs");
        (endpoints, Mutex::new(child))
    });
    let harness = Harness {
        endpoints: endpoints.clone(),
        http: reqwest::Client::new(),
    };
    harness.post("/reset", &json!({})).await;
    harness
}

impl Harness {
    pub fn ws_url(&self) -> &str {
        &self.endpoints.ws_url
    }

    pub fn client(&self) -> Client {
        Client::builder()
            .base_url(&self.endpoints.control_url)
            .publishable_key("pk_test-key")
            .access_token("test-token")
            .build()
            .expect("harness SDK client")
    }

    pub async fn socket(&self) -> archastro::Socket {
        archastro::SocketBuilder::new(&self.endpoints.ws_url)
            .connect()
            .await
            .expect("harness socket")
    }

    pub async fn register_stream(&self, route: &str, events: &[&str]) {
        let actions: Vec<_> = events
            .iter()
            .map(|event| json!({ "type": "autoEmit", "event": event }))
            .collect();
        self.post(
            "/stream-scenarios",
            &json!({ "route": route, "actions": actions }),
        )
        .await;
    }

    pub async fn register_stream_actions(&self, route: &str, actions: &[serde_json::Value]) {
        self.post(
            "/stream-scenarios",
            &json!({ "route": route, "actions": actions }),
        )
        .await;
    }

    pub async fn register_channel(&self, topic: &str, messages: &[&str], pushes: &[&str]) {
        let on_message = messages
            .iter()
            .map(|event| ((*event).to_owned(), json!([{ "type": "autoReply" }])))
            .collect::<serde_json::Map<_, _>>();
        let mut on_join = vec![json!({ "type": "autoReply" })];
        on_join.extend(
            pushes
                .iter()
                .map(|event| json!({ "type": "autoPush", "event": event })),
        );
        self.post(
            "/scenarios",
            &json!({ "topic": topic, "onJoin": on_join, "onMessage": on_message }),
        )
        .await;
    }

    pub async fn register_scenario(&self, scenario: &serde_json::Value) {
        self.post("/scenarios", scenario).await;
    }

    async fn post(&self, path: &str, body: &serde_json::Value) {
        let response = self
            .http
            .post(format!("{}{}", self.endpoints.control_url, path))
            .json(body)
            .send()
            .await
            .expect("harness request");
        assert!(
            response.status().is_success(),
            "harness {path} returned {}",
            response.status()
        );
    }
}
