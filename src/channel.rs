use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_core::Stream;
use futures_util::{SinkExt, StreamExt};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tokio::sync::{Mutex, broadcast, oneshot};
use tokio_stream::wrappers::BroadcastStream;
use tokio_tungstenite::tungstenite::Message;

use crate::{ChannelError, Error, Result};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_HEARTBEAT: Duration = Duration::from_secs(30);
const BUFFER_CAPACITY: usize = 32;
const DEFAULT_BACKOFF: &[Duration] = &[
    Duration::from_millis(10),
    Duration::from_millis(50),
    Duration::from_millis(100),
    Duration::from_millis(150),
    Duration::from_millis(200),
    Duration::from_millis(250),
    Duration::from_millis(500),
    Duration::from_secs(1),
    Duration::from_secs(2),
];

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
type Writer = futures_util::stream::SplitSink<Ws, Message>;
type Reader = futures_util::stream::SplitStream<Ws>;
type EventKey = (String, String);

/// Phoenix channel lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelState {
    /// Not joined.
    Closed,
    /// Join request is in flight.
    Joining,
    /// Joined and ready for pushes.
    Joined,
    /// Leave request is in flight.
    Leaving,
    /// Join or transport failed.
    Errored,
}

/// Socket lifecycle notification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SocketEvent {
    /// The transport connected or reconnected.
    Open,
    /// The transport closed.
    Close {
        /// WebSocket close code, when supplied.
        code: Option<u16>,
        /// WebSocket close reason.
        reason: String,
    },
    /// A connection attempt failed.
    Error(String),
}

/// Configures and connects a Phoenix socket.
pub struct SocketBuilder {
    url: String,
    params: Vec<(String, String)>,
    timeout: Duration,
    heartbeat: Duration,
    reconnect_backoff: Vec<Duration>,
    auto_reconnect: bool,
}

impl SocketBuilder {
    /// Create a builder for a Phoenix WebSocket URL.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            params: Vec::new(),
            timeout: DEFAULT_TIMEOUT,
            heartbeat: DEFAULT_HEARTBEAT,
            reconnect_backoff: DEFAULT_BACKOFF.to_vec(),
            auto_reconnect: true,
        }
    }

    /// Add a socket connect parameter.
    pub fn param(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.params.push((key.into(), value.into()));
        self
    }

    /// Set join/push/leave timeout.
    pub fn timeout(mut self, value: Duration) -> Self {
        self.timeout = value;
        self
    }

    /// Set Phoenix heartbeat interval.
    pub fn heartbeat(mut self, value: Duration) -> Self {
        self.heartbeat = value;
        self
    }

    /// Enable or disable automatic transport reconnect and channel rejoin.
    pub fn auto_reconnect(mut self, value: bool) -> Self {
        self.auto_reconnect = value;
        self
    }

    /// Replace the reconnect delay schedule.
    pub fn reconnect_backoff(mut self, value: impl IntoIterator<Item = Duration>) -> Self {
        self.reconnect_backoff = value.into_iter().collect();
        self
    }

    /// Connect and start receive, reconnect, and heartbeat tasks.
    pub async fn connect(self) -> Result<Socket> {
        let mut url = url::Url::parse(&self.url)?;
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("vsn", "2.0.0");
            for (key, value) in self.params {
                query.append_pair(&key, &value);
            }
        }
        let (writer, reader) = connect_once(url.as_str()).await?;
        let (socket_events, _) = broadcast::channel(BUFFER_CAPACITY);
        let inner = Arc::new(SocketInner {
            url: url.into(),
            writer: Mutex::new(Some(writer)),
            pending: Mutex::new(HashMap::new()),
            events: Mutex::new(HashMap::new()),
            buffered: Mutex::new(HashMap::new()),
            channels: std::sync::Mutex::new(HashMap::new()),
            socket_events,
            refs: AtomicU64::new(0),
            connected: AtomicBool::new(true),
            closing: AtomicBool::new(false),
            reconnecting: AtomicBool::new(false),
            timeout: self.timeout,
            heartbeat: self.heartbeat,
            reconnect_backoff: if self.reconnect_backoff.is_empty() {
                DEFAULT_BACKOFF.to_vec()
            } else {
                self.reconnect_backoff
            },
            auto_reconnect: self.auto_reconnect,
        });
        spawn_reader(Arc::clone(&inner), reader);
        tokio::spawn(heartbeat_loop(Arc::clone(&inner)));
        let _ = inner.socket_events.send(SocketEvent::Open);
        Ok(Socket { inner })
    }
}

struct SocketInner {
    url: String,
    writer: Mutex<Option<Writer>>,
    pending: Mutex<HashMap<String, oneshot::Sender<Result<Value>>>>,
    events: Mutex<HashMap<EventKey, broadcast::Sender<Value>>>,
    buffered: Mutex<HashMap<EventKey, Vec<Value>>>,
    channels: std::sync::Mutex<HashMap<String, std::sync::Weak<ChannelInner>>>,
    socket_events: broadcast::Sender<SocketEvent>,
    refs: AtomicU64,
    connected: AtomicBool,
    closing: AtomicBool,
    reconnecting: AtomicBool,
    timeout: Duration,
    heartbeat: Duration,
    reconnect_backoff: Vec<Duration>,
    auto_reconnect: bool,
}

/// A connected Phoenix socket. Clone it freely across tasks.
#[derive(Clone)]
pub struct Socket {
    inner: Arc<SocketInner>,
}

impl Socket {
    /// Return the existing channel for a topic or create one.
    pub fn channel(&self, topic: impl Into<String>) -> Channel {
        let topic = topic.into();
        let mut channels = self
            .inner
            .channels
            .lock()
            .expect("channel registry poisoned");
        if let Some(existing) = channels.get(&topic).and_then(std::sync::Weak::upgrade) {
            return Channel { inner: existing };
        }
        let inner = Arc::new(ChannelInner {
            socket: self.clone(),
            topic: topic.clone(),
            state: Mutex::new(ChannelState::Closed),
            join_ref: Mutex::new(None),
            join_payload: Mutex::new(json!({})),
            join_gate: Mutex::new(()),
            desired_join: AtomicBool::new(false),
            buffered_pushes: Mutex::new(Vec::new()),
        });
        channels.insert(topic, Arc::downgrade(&inner));
        Channel { inner }
    }

    /// Whether the underlying WebSocket is currently open.
    pub fn is_connected(&self) -> bool {
        self.inner.connected.load(Ordering::Acquire)
    }

    /// Subscribe to socket open, close, and connection-error events.
    pub fn events(&self) -> SocketEventStream {
        SocketEventStream {
            inner: BroadcastStream::new(self.inner.socket_events.subscribe()),
        }
    }

    /// Gracefully close the WebSocket and disable reconnection.
    pub async fn close(&self) -> Result<()> {
        self.inner.closing.store(true, Ordering::Release);
        self.inner.connected.store(false, Ordering::Release);
        if let Some(mut writer) = self.inner.writer.lock().await.take() {
            writer.close().await?;
        }
        Ok(())
    }

    fn next_ref(&self) -> String {
        self.inner
            .refs
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1)
            .to_string()
    }

    async fn request(
        &self,
        join_ref: Option<&str>,
        topic: &str,
        event: &str,
        payload: Value,
    ) -> Result<(String, Value)> {
        if !self.is_connected() {
            return Err(Error::Closed);
        }
        let reference = self.next_ref();
        let frame = json!([join_ref, reference, topic, event, payload]);
        let (sender, receiver) = oneshot::channel();
        self.inner
            .pending
            .lock()
            .await
            .insert(reference.clone(), sender);
        let send_result = {
            let mut writer = self.inner.writer.lock().await;
            match writer.as_mut() {
                Some(writer) => writer.send(Message::Text(frame.to_string().into())).await,
                None => {
                    self.inner.pending.lock().await.remove(&reference);
                    return Err(Error::Closed);
                }
            }
        };
        if let Err(error) = send_result {
            self.inner.pending.lock().await.remove(&reference);
            return Err(error.into());
        }
        let response = match tokio::time::timeout(self.inner.timeout, receiver).await {
            Ok(response) => response.map_err(|_| Error::Closed)??,
            Err(_) => {
                self.inner.pending.lock().await.remove(&reference);
                return Err(Error::Timeout);
            }
        };
        Ok((reference, response))
    }

    async fn subscribe<T: DeserializeOwned + Send + 'static>(
        &self,
        topic: &str,
        event: &str,
    ) -> ChannelEventStream<T> {
        let key = (topic.to_owned(), event.to_owned());
        let sender = {
            let mut events = self.inner.events.lock().await;
            events
                .entry(key.clone())
                .or_insert_with(|| broadcast::channel(BUFFER_CAPACITY).0)
                .clone()
        };
        let receiver = sender.subscribe();
        if let Some(values) = self.inner.buffered.lock().await.remove(&key) {
            for value in values {
                let _ = sender.send(value);
            }
        }
        ChannelEventStream {
            inner: BroadcastStream::new(receiver),
            marker: std::marker::PhantomData,
        }
    }
}

struct BufferedPush {
    reference: String,
    event: String,
    payload: Value,
    sender: oneshot::Sender<Result<Value>>,
}

struct ChannelInner {
    socket: Socket,
    topic: String,
    state: Mutex<ChannelState>,
    join_ref: Mutex<Option<String>>,
    join_payload: Mutex<Value>,
    join_gate: Mutex<()>,
    desired_join: AtomicBool,
    buffered_pushes: Mutex<Vec<BufferedPush>>,
}

/// A joined Phoenix channel.
#[derive(Clone)]
pub struct Channel {
    inner: Arc<ChannelInner>,
}

impl Channel {
    /// Topic string.
    pub fn topic(&self) -> &str {
        &self.inner.topic
    }

    /// Current local lifecycle state.
    pub async fn state(&self) -> ChannelState {
        *self.inner.state.lock().await
    }

    /// Join and return the server response payload.
    pub async fn join(&self, payload: Value) -> Result<Value> {
        *self.inner.join_payload.lock().await = payload;
        self.inner.desired_join.store(true, Ordering::Release);
        self.join_saved().await
    }

    async fn join_saved(&self) -> Result<Value> {
        let _gate = self.inner.join_gate.lock().await;
        if *self.inner.state.lock().await == ChannelState::Joined {
            return Ok(json!({}));
        }
        *self.inner.state.lock().await = ChannelState::Joining;
        let payload = self.inner.join_payload.lock().await.clone();
        let result = self
            .inner
            .socket
            .request(None, &self.inner.topic, "phx_join", payload)
            .await;
        let (reference, envelope) = match result {
            Ok(value) => value,
            Err(error) => {
                *self.inner.state.lock().await = ChannelState::Errored;
                return Err(error);
            }
        };
        match reply(&self.inner.topic, "join", &envelope) {
            Ok(value) => {
                *self.inner.join_ref.lock().await = Some(reference);
                *self.inner.state.lock().await = ChannelState::Joined;
                self.flush_pushes().await;
                Ok(value)
            }
            Err(error) => {
                *self.inner.state.lock().await = ChannelState::Errored;
                Err(error)
            }
        }
    }

    /// Push an event and return its reply response.
    ///
    /// Pushes made while a desired channel is reconnecting are buffered until
    /// its join succeeds or the normal operation timeout expires.
    pub async fn push(&self, event: &str, payload: Value) -> Result<Value> {
        if *self.inner.state.lock().await != ChannelState::Joined {
            if !self.inner.desired_join.load(Ordering::Acquire) {
                return Err(channel_error(
                    &self.inner.topic,
                    event,
                    "channel is not joined",
                ));
            }
            let (sender, receiver) = oneshot::channel();
            let reference = self.inner.socket.next_ref();
            self.inner.buffered_pushes.lock().await.push(BufferedPush {
                reference: reference.clone(),
                event: event.to_owned(),
                payload,
                sender,
            });
            return match tokio::time::timeout(self.inner.socket.inner.timeout, receiver).await {
                Ok(result) => result.map_err(|_| Error::Closed)?,
                Err(_) => {
                    self.inner
                        .buffered_pushes
                        .lock()
                        .await
                        .retain(|push| push.reference != reference);
                    Err(Error::Timeout)
                }
            };
        }
        self.push_now(event, payload).await
    }

    async fn push_now(&self, event: &str, payload: Value) -> Result<Value> {
        let join_ref = self.inner.join_ref.lock().await.clone();
        let (_, envelope) = self
            .inner
            .socket
            .request(join_ref.as_deref(), &self.inner.topic, event, payload)
            .await?;
        reply(&self.inner.topic, event, &envelope)
    }

    async fn flush_pushes(&self) {
        let pushes = std::mem::take(&mut *self.inner.buffered_pushes.lock().await);
        for push in pushes {
            if push.sender.is_closed() {
                continue;
            }
            let channel = self.clone();
            tokio::spawn(async move {
                let result = channel.push_now(&push.event, push.payload).await;
                let _ = push.sender.send(result);
            });
        }
    }

    /// Leave the topic. A timed-out leave is treated as successful because the
    /// server tears down the subscription regardless.
    pub async fn leave(&self) -> Result<()> {
        self.inner.desired_join.store(false, Ordering::Release);
        if *self.inner.state.lock().await == ChannelState::Closed {
            return Ok(());
        }
        *self.inner.state.lock().await = ChannelState::Leaving;
        let join_ref = self.inner.join_ref.lock().await.clone();
        let result = self
            .inner
            .socket
            .request(
                join_ref.as_deref(),
                &self.inner.topic,
                "phx_leave",
                json!({}),
            )
            .await;
        *self.inner.join_ref.lock().await = None;
        *self.inner.state.lock().await = ChannelState::Closed;
        self.inner
            .socket
            .inner
            .channels
            .lock()
            .expect("channel registry poisoned")
            .remove(&self.inner.topic);
        match result {
            Ok(_) | Err(Error::Timeout | Error::Closed) => Ok(()),
            Err(error) => Err(error),
        }
    }

    /// Subscribe to a typed server push.
    pub fn subscribe<T: DeserializeOwned + Send + 'static>(
        &self,
        event: &str,
    ) -> ChannelEventStream<T> {
        ChannelEventStream::pending(
            self.inner.socket.clone(),
            self.inner.topic.clone(),
            event.to_owned(),
        )
    }
}

/// Stream of socket lifecycle events.
pub struct SocketEventStream {
    inner: BroadcastStream<SocketEvent>,
}

impl Stream for SocketEventStream {
    type Item = SocketEvent;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match Pin::new(&mut self.inner).poll_next(cx) {
                Poll::Ready(Some(Ok(event))) => return Poll::Ready(Some(event)),
                Poll::Ready(Some(Err(_))) => continue,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// Stream of typed Phoenix server-push payloads.
pub struct ChannelEventStream<T> {
    inner: BroadcastStream<Value>,
    marker: std::marker::PhantomData<T>,
}

impl<T: DeserializeOwned + Send + 'static> ChannelEventStream<T> {
    fn pending(socket: Socket, topic: String, event: String) -> Self {
        let (forward, forwarded) = broadcast::channel(BUFFER_CAPACITY);
        tokio::spawn(async move {
            let mut source = socket.subscribe::<Value>(&topic, &event).await;
            while let Some(value) = source.next().await {
                if let Ok(value) = value {
                    let _ = forward.send(value);
                }
            }
        });
        Self {
            inner: BroadcastStream::new(forwarded),
            marker: std::marker::PhantomData,
        }
    }
}

impl<T: DeserializeOwned + Unpin> Stream for ChannelEventStream<T> {
    type Item = Result<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(value))) => {
                Poll::Ready(Some(serde_json::from_value(value).map_err(Error::from)))
            }
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(Error::Channel(ChannelError {
                operation: "receive".into(),
                topic: String::new(),
                reason: error.to_string(),
                payload: None,
            })))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

fn reply(topic: &str, operation: &str, envelope: &Value) -> Result<Value> {
    let status = envelope
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("error");
    let response = envelope.get("response").cloned().unwrap_or(Value::Null);
    if status == "ok" {
        Ok(response)
    } else {
        Err(ChannelError {
            operation: operation.to_owned(),
            topic: topic.to_owned(),
            reason: "server rejected the operation".into(),
            payload: Some(response),
        }
        .into())
    }
}

fn channel_error(topic: &str, operation: &str, reason: &str) -> Error {
    ChannelError {
        operation: operation.to_owned(),
        topic: topic.to_owned(),
        reason: reason.to_owned(),
        payload: None,
    }
    .into()
}

async fn connect_once(url: &str) -> Result<(Writer, Reader)> {
    let (stream, _) = tokio_tungstenite::connect_async(url).await?;
    Ok(stream.split())
}

fn spawn_reader(inner: Arc<SocketInner>, reader: Reader) {
    tokio::spawn(async move {
        read_loop(Arc::clone(&inner), reader).await;
        handle_disconnect(Arc::clone(&inner)).await;
        schedule_reconnect(inner);
    });
}

async fn read_loop(inner: Arc<SocketInner>, mut reader: Reader) {
    let mut close = (None, String::new());
    while let Some(message) = reader.next().await {
        let message = match message {
            Ok(value) => value,
            Err(error) => {
                let _ = inner
                    .socket_events
                    .send(SocketEvent::Error(error.to_string()));
                break;
            }
        };
        if let Message::Close(frame) = message {
            if let Some(frame) = frame {
                close = (Some(frame.code.into()), frame.reason.to_string());
            }
            break;
        }
        let Message::Text(text) = message else {
            continue;
        };
        let Ok(Value::Array(frame)) = serde_json::from_str::<Value>(&text) else {
            continue;
        };
        if frame.len() != 5 {
            continue;
        }
        let reference = frame[1].as_str().unwrap_or_default();
        let topic = frame[2].as_str().unwrap_or_default().to_owned();
        let event = frame[3].as_str().unwrap_or_default().to_owned();
        let payload = frame[4].clone();
        if event == "phx_reply" && !reference.is_empty() {
            if let Some(sender) = inner.pending.lock().await.remove(reference) {
                let _ = sender.send(Ok(payload));
            }
            continue;
        }
        if let Some(channel) = channel_for(&inner, &topic) {
            if !join_ref_matches(frame[0].as_str(), channel.join_ref.lock().await.as_deref()) {
                continue;
            }
        }
        if event == "phx_error" {
            if let Some(channel) = channel_for(&inner, &topic) {
                *channel.state.lock().await = ChannelState::Errored;
            }
        } else if event == "phx_close" {
            if let Some(channel) = channel_for(&inner, &topic) {
                channel.desired_join.store(false, Ordering::Release);
                *channel.state.lock().await = ChannelState::Closed;
            }
        }
        let key = (topic, event);
        if let Some(sender) = inner.events.lock().await.get(&key).cloned() {
            let _ = sender.send(payload);
        } else {
            let mut buffered = inner.buffered.lock().await;
            let values = buffered.entry(key).or_default();
            if values.len() == BUFFER_CAPACITY {
                values.remove(0);
            }
            values.push(payload);
        }
    }
    let _ = inner.socket_events.send(SocketEvent::Close {
        code: close.0,
        reason: close.1,
    });
}

async fn handle_disconnect(inner: Arc<SocketInner>) {
    if !inner.connected.swap(false, Ordering::AcqRel) {
        return;
    }
    inner.writer.lock().await.take();
    let pending = std::mem::take(&mut *inner.pending.lock().await);
    for (_, sender) in pending {
        let _ = sender.send(Err(Error::Closed));
    }
    let channels = live_channels(&inner);
    for channel in channels {
        let state = *channel.state.lock().await;
        if state != ChannelState::Closed && state != ChannelState::Leaving {
            *channel.state.lock().await = ChannelState::Errored;
            *channel.join_ref.lock().await = None;
        }
    }
}

fn schedule_reconnect(inner: Arc<SocketInner>) {
    if inner.closing.load(Ordering::Acquire)
        || !inner.auto_reconnect
        || inner.reconnecting.swap(true, Ordering::AcqRel)
    {
        return;
    }
    tokio::spawn(async move {
        let mut attempt = 0_usize;
        loop {
            let delay = reconnect_delay(&inner.reconnect_backoff, attempt);
            attempt = attempt.saturating_add(1);
            tokio::time::sleep(delay).await;
            if inner.closing.load(Ordering::Acquire) {
                break;
            }
            match connect_once(&inner.url).await {
                Ok((writer, reader)) => {
                    *inner.writer.lock().await = Some(writer);
                    inner.connected.store(true, Ordering::Release);
                    inner.reconnecting.store(false, Ordering::Release);
                    spawn_reader(Arc::clone(&inner), reader);
                    let _ = inner.socket_events.send(SocketEvent::Open);
                    rejoin_channels(&inner).await;
                    return;
                }
                Err(error) => {
                    let _ = inner
                        .socket_events
                        .send(SocketEvent::Error(error.to_string()));
                }
            }
        }
        inner.reconnecting.store(false, Ordering::Release);
    });
}

fn join_ref_matches(frame: Option<&str>, current: Option<&str>) -> bool {
    frame.is_none() || frame == current
}

fn reconnect_delay(schedule: &[Duration], attempt: usize) -> Duration {
    schedule[attempt.min(schedule.len().saturating_sub(1))]
}

async fn rejoin_channels(inner: &Arc<SocketInner>) {
    for channel in live_channels(inner) {
        if channel.desired_join.load(Ordering::Acquire) {
            let channel = Channel { inner: channel };
            let _ = channel.join_saved().await;
        }
    }
}

fn channel_for(inner: &SocketInner, topic: &str) -> Option<Arc<ChannelInner>> {
    inner
        .channels
        .lock()
        .expect("channel registry poisoned")
        .get(topic)
        .and_then(std::sync::Weak::upgrade)
}

fn live_channels(inner: &SocketInner) -> Vec<Arc<ChannelInner>> {
    let mut channels = inner.channels.lock().expect("channel registry poisoned");
    channels.retain(|_, channel| channel.strong_count() > 0);
    channels
        .values()
        .filter_map(std::sync::Weak::upgrade)
        .collect()
}

async fn heartbeat_loop(inner: Arc<SocketInner>) {
    let mut ticker = tokio::time::interval(inner.heartbeat);
    ticker.tick().await;
    loop {
        ticker.tick().await;
        if inner.closing.load(Ordering::Acquire) {
            return;
        }
        if !inner.connected.load(Ordering::Acquire) {
            continue;
        }
        let socket = Socket {
            inner: Arc::clone(&inner),
        };
        if socket
            .request(None, "phoenix", "heartbeat", json!({}))
            .await
            .is_err()
        {
            if let Some(mut writer) = inner.writer.lock().await.take() {
                let _ = writer.close().await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;
    use tokio_tungstenite::accept_async;

    async fn silent_socket(timeout: Duration) -> (Socket, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("local address");
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut websocket = accept_async(stream).await.expect("websocket handshake");
            while websocket.next().await.is_some() {}
        });
        let socket = SocketBuilder::new(format!("ws://{address}/socket/api/websocket"))
            .timeout(timeout)
            .heartbeat(Duration::from_secs(60))
            .auto_reconnect(false)
            .connect()
            .await
            .expect("connect");
        (socket, server)
    }

    #[test]
    fn stale_join_refs_are_rejected_after_rejoin() {
        assert!(join_ref_matches(None, Some("new")));
        assert!(join_ref_matches(Some("new"), Some("new")));
        assert!(!join_ref_matches(Some("old"), Some("new")));
        assert!(!join_ref_matches(Some("old"), None));
    }

    #[test]
    fn reconnect_backoff_clamps_to_the_last_delay() {
        let schedule = [Duration::from_millis(10), Duration::from_secs(2)];
        assert_eq!(reconnect_delay(&schedule, 0), schedule[0]);
        assert_eq!(reconnect_delay(&schedule, 1), schedule[1]);
        assert_eq!(reconnect_delay(&schedule, 99), schedule[1]);
    }

    #[tokio::test]
    async fn request_timeout_removes_pending_reply() {
        let (socket, server) = silent_socket(Duration::from_millis(20)).await;
        let result = socket.request(None, "topic", "event", json!({})).await;
        assert!(matches!(result, Err(Error::Timeout)));
        assert!(socket.inner.pending.lock().await.is_empty());
        socket.close().await.expect("close");
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn buffered_push_timeout_removes_the_queued_operation() {
        let (socket, server) = silent_socket(Duration::from_millis(20)).await;
        let channel = socket.channel("topic");
        channel.inner.desired_join.store(true, Ordering::Release);
        *channel.inner.state.lock().await = ChannelState::Errored;

        let result = channel.push("save", json!({})).await;
        assert!(matches!(result, Err(Error::Timeout)));
        assert!(channel.inner.buffered_pushes.lock().await.is_empty());

        socket.close().await.expect("close");
        server.await.expect("server task");
    }
}
