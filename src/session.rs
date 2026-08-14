use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Durable app-user session suitable for secure storage.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AppSession {
    /// Bearer access token.
    pub access_token: String,
    /// Single-use refresh token used to renew the session.
    pub refresh_token: Option<String>,
    /// Access-token expiry as Unix milliseconds, when known.
    pub access_token_expires_at: Option<i64>,
    /// Authenticated user snapshot, when supplied by the login flow.
    pub user: Option<Value>,
}

/// Async durable storage used by app clients.
///
/// Mobile applications commonly implement this with a secure keychain;
/// servers may use an encrypted database or another application-owned store.
#[async_trait::async_trait]
pub trait SessionStore: Send + Sync {
    /// Load a previously persisted session.
    async fn load(
        &self,
    ) -> std::result::Result<Option<AppSession>, Box<dyn std::error::Error + Send + Sync>>;

    /// Persist the current session, including rotated refresh credentials.
    async fn save(
        &self,
        session: &AppSession,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// Remove any persisted session.
    async fn clear(&self) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;
}
