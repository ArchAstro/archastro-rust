use std::collections::BTreeMap;
use std::sync::Arc;

use tokio::sync::{Mutex, RwLock};

use crate::generated::{Auth, V1};
use crate::{AppSession, Error, RequestBuilder, Result, SessionStore, SocketBuilder};

const DEFAULT_BASE_URL: &str = "https://platform.archastro.ai";

#[derive(Debug, Clone, Default)]
pub(crate) struct Session {
    pub access_token: Option<String>,
    pub refresh_token: Option<String>,
    pub refresh_path: Option<String>,
    pub access_token_expires_at: Option<i64>,
    pub user: Option<serde_json::Value>,
    pub generation: u64,
}

pub(crate) struct ClientInner {
    pub base_url: String,
    pub http: reqwest::Client,
    pub headers: BTreeMap<String, String>,
    pub session: RwLock<Session>,
    pub refresh_gate: Mutex<()>,
    pub session_store: Option<Arc<dyn SessionStore>>,
}

/// Cloneable asynchronous ArchAstro client.
#[derive(Clone)]
pub struct Client(pub(crate) Arc<ClientInner>);

/// Builder for [`Client`].
pub struct ClientBuilder {
    base_url: String,
    http: Option<reqwest::Client>,
    headers: BTreeMap<String, String>,
    access_token: Option<String>,
    session_store: Option<Arc<dyn SessionStore>>,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self {
            base_url: DEFAULT_BASE_URL.to_owned(),
            http: None,
            headers: BTreeMap::new(),
            access_token: None,
            session_store: None,
        }
    }
}

impl ClientBuilder {
    /// Override the API origin (primarily for private deployments and tests).
    pub fn base_url(mut self, value: impl Into<String>) -> Self {
        self.base_url = value.into().trim_end_matches('/').to_owned();
        self
    }

    /// Use a preconfigured Reqwest client.
    pub fn http_client(mut self, value: reqwest::Client) -> Self {
        self.http = Some(value);
        self
    }

    /// Add a default request header.
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(name.into(), value.into());
        self
    }

    /// Authenticate with a secret API key.
    pub fn secret_key(self, value: impl Into<String>) -> Self {
        self.header("x-archastro-api-key", value)
    }

    /// Configure the publishable key used by app-user auth flows.
    pub fn publishable_key(self, value: impl Into<String>) -> Self {
        self.header("x-archastro-api-key", value)
    }

    /// Use an existing bearer/system-user token.
    pub fn access_token(mut self, value: impl Into<String>) -> Self {
        self.access_token = Some(value.into());
        self
    }

    /// Persist app-user sessions and refresh-token rotations in this store.
    pub fn session_store(mut self, value: Arc<dyn SessionStore>) -> Self {
        self.session_store = Some(value);
        self
    }

    /// Build a client and validate its base URL.
    pub fn build(self) -> Result<Client> {
        let _ = url::Url::parse(&self.base_url)?;
        let http = match self.http {
            Some(client) => client,
            None => reqwest::Client::builder()
                .connect_timeout(std::time::Duration::from_secs(10))
                .tcp_keepalive(std::time::Duration::from_secs(30))
                .build()?,
        };
        Ok(Client(Arc::new(ClientInner {
            base_url: self.base_url,
            http,
            headers: self.headers,
            session: RwLock::new(Session {
                access_token: self.access_token,
                ..Session::default()
            }),
            refresh_gate: Mutex::new(()),
            session_store: self.session_store,
        })))
    }
}

impl Client {
    /// Start building a client.
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    /// Build a client with defaults and no authentication.
    pub fn new() -> Result<Self> {
        Self::builder().build()
    }

    /// Version 1 API namespace.
    pub fn v1(&self) -> V1 {
        V1 {
            client: self.clone(),
        }
    }

    /// Authentication endpoints.
    pub fn auth(&self) -> Auth {
        Auth {
            client: self.clone(),
        }
    }

    /// Start a typed HTTP request used by generated resources.
    pub fn request(&self, method: reqwest::Method, path: &str) -> RequestBuilder {
        RequestBuilder::new(self.clone(), method, path)
    }

    /// Build a Phoenix socket authenticated from the client's current session.
    pub async fn socket(&self) -> Result<SocketBuilder> {
        let session = self.0.session.read().await;
        let token = session.access_token.clone().ok_or_else(|| {
            Error::Configuration("a bearer token is required for channels".into())
        })?;
        let mut builder =
            SocketBuilder::new(websocket_url(&self.0.base_url)?).param("token", token);
        if let Some(key) = self
            .0
            .headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case("x-archastro-api-key"))
            .map(|(_, value)| value)
        {
            builder = builder.param("api_key", key.clone());
        }
        Ok(builder)
    }

    /// Replace the access/refresh token pair used by this client.
    pub async fn install_session(
        &self,
        access_token: String,
        refresh_token: Option<String>,
        refresh_path: &str,
    ) {
        let mut session = self.0.session.write().await;
        session.access_token = Some(access_token);
        session.refresh_token = refresh_token;
        session.refresh_path = Some(refresh_path.to_owned());
        session.generation = session.generation.wrapping_add(1);
    }

    /// Install and persist a complete app-user session.
    pub async fn install_app_session(&self, value: AppSession) -> Result<()> {
        {
            let mut session = self.0.session.write().await;
            session.access_token = Some(value.access_token.clone());
            session.refresh_token = value.refresh_token.clone();
            session.refresh_path = Some("/api/v1/auth/refresh".to_owned());
            session.access_token_expires_at = value.access_token_expires_at;
            session.user.clone_from(&value.user);
            session.generation = session.generation.wrapping_add(1);
        }
        if let Some(store) = &self.0.session_store {
            store
                .save(&value)
                .await
                .map_err(|error| Error::SessionStorage(error.to_string()))?;
        }
        Ok(())
    }

    /// Restore an app-user session from the configured store.
    ///
    /// A session without a refresh token is cleared because it cannot renew.
    pub async fn restore_session(&self) -> Result<Option<AppSession>> {
        let Some(store) = &self.0.session_store else {
            return Err(Error::Configuration(
                "no session store is configured".into(),
            ));
        };
        let Some(value) = store
            .load()
            .await
            .map_err(|error| Error::SessionStorage(error.to_string()))?
        else {
            return Ok(None);
        };
        if value.refresh_token.is_none() {
            store
                .clear()
                .await
                .map_err(|error| Error::SessionStorage(error.to_string()))?;
            return Ok(None);
        }
        self.install_app_session(value.clone()).await?;
        Ok(Some(value))
    }

    /// Return the current app-user session, including refresh credentials.
    ///
    /// Treat this value as sensitive and persist it only in secure storage.
    pub async fn app_session(&self) -> Option<AppSession> {
        let session = self.0.session.read().await;
        Some(AppSession {
            access_token: session.access_token.clone()?,
            refresh_token: session.refresh_token.clone(),
            access_token_expires_at: session.access_token_expires_at,
            user: session.user.clone(),
        })
    }

    /// Clear the in-memory and persisted app-user session.
    pub async fn sign_out(&self) -> Result<()> {
        {
            let mut session = self.0.session.write().await;
            *session = Session {
                generation: session.generation.wrapping_add(1),
                ..Session::default()
            };
        }
        if let Some(store) = &self.0.session_store {
            store
                .clear()
                .await
                .map_err(|error| Error::SessionStorage(error.to_string()))?;
        }
        Ok(())
    }

    /// Return the current bearer token without exposing refresh credentials.
    pub async fn access_token(&self) -> Option<String> {
        self.0.session.read().await.access_token.clone()
    }
}

fn websocket_url(base_url: &str) -> Result<String> {
    let mut url = url::Url::parse(base_url)?;
    url.set_scheme(if url.scheme() == "https" { "wss" } else { "ws" })
        .map_err(|()| Error::Configuration("unsupported base URL scheme".into()))?;
    url.set_path("/socket/api/websocket");
    Ok(url.to_string())
}

#[cfg(test)]
mod tests {
    use super::websocket_url;

    #[test]
    fn websocket_url_uses_the_platform_channel_endpoint() {
        assert_eq!(
            websocket_url("https://platform.archastro.ai/api").expect("valid URL"),
            "wss://platform.archastro.ai/socket/api/websocket"
        );
        assert_eq!(
            websocket_url("http://localhost:4005").expect("valid URL"),
            "ws://localhost:4005/socket/api/websocket"
        );
    }
}
