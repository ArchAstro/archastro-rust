use serde_json::Value;

/// Result type used throughout the SDK.
pub type Result<T> = std::result::Result<T, Error>;

/// A structured non-success response from the ArchAstro API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiError {
    /// HTTP status code.
    pub status: u16,
    /// Stable machine-readable error code, when supplied.
    pub code: Option<String>,
    /// Human-readable message.
    pub message: String,
    /// Full decoded response body.
    pub body: Value,
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ArchAstro API returned HTTP {}: {}",
            self.status, self.message
        )
    }
}

impl std::error::Error for ApiError {}

/// A Phoenix channel lifecycle or protocol failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("channel {operation} on {topic:?} failed: {reason}")]
pub struct ChannelError {
    /// Operation being attempted.
    pub operation: String,
    /// Phoenix topic.
    pub topic: String,
    /// Failure description.
    pub reason: String,
    /// Rejection payload, when supplied.
    pub payload: Option<Value>,
}

/// Every error surfaced by the SDK.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Structured API error.
    #[error(transparent)]
    Api(#[from] ApiError),
    /// HTTP transport error.
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    /// JSON codec error.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// Query-string codec error.
    #[error(transparent)]
    Query(#[from] serde_urlencoded::ser::Error),
    /// URL parse error.
    #[error(transparent)]
    Url(#[from] url::ParseError),
    /// WebSocket transport error.
    #[error(transparent)]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
    /// Phoenix channel error.
    #[error(transparent)]
    Channel(#[from] ChannelError),
    /// A generated SSE stream received an event absent from its contract.
    #[error("unknown SSE event {0:?}")]
    UnknownSseEvent(String),
    /// An SSE transport or protocol error.
    #[error("SSE stream failed: {0}")]
    Sse(String),
    /// Invalid SDK configuration.
    #[error("invalid SDK configuration: {0}")]
    Configuration(String),
    /// Durable app-session storage failed.
    #[error("session storage failed: {0}")]
    SessionStorage(String),
    /// A request timed out.
    #[error("operation timed out")]
    Timeout,
    /// A background connection task stopped unexpectedly.
    #[error("connection closed")]
    Closed,
}
