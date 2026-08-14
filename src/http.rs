use futures_util::StreamExt;
use reqwest::header::{HeaderName, HeaderValue};
use reqwest_eventsource::{Event, RequestBuilderExt};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use crate::sse::{SseDecode, SseStream};
use crate::{ApiError, Client, Error, Result};

/// Raw bytes returned by download/export endpoints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawResponse {
    /// Body bytes.
    pub bytes: Vec<u8>,
    /// Response MIME type, when supplied.
    pub content_type: Option<String>,
}

/// Fluent request assembled by generated resource methods.
pub struct RequestBuilder {
    client: Client,
    method: reqwest::Method,
    path: String,
    query: Option<String>,
    body: Option<Value>,
}

impl RequestBuilder {
    pub(crate) fn new(client: Client, method: reqwest::Method, path: &str) -> Self {
        Self {
            client,
            method,
            path: path.to_owned(),
            query: None,
            body: None,
        }
    }

    /// Serialize query parameters according to `application/x-www-form-urlencoded` rules.
    pub fn query(mut self, value: &impl Serialize) -> Result<Self> {
        self.query = Some(serde_urlencoded::to_string(value)?);
        Ok(self)
    }

    /// Serialize a JSON request body.
    pub fn json(mut self, value: &impl Serialize) -> Result<Self> {
        self.body = Some(serde_json::to_value(value)?);
        Ok(self)
    }

    /// Send and decode a JSON response.
    pub async fn send<T: DeserializeOwned>(self) -> Result<T> {
        let response = self.execute().await?;
        let status = response.status();
        let bytes = response.bytes().await?;
        if bytes.is_empty() {
            return Err(Error::Api(ApiError {
                status: status.as_u16(),
                code: Some("empty_response".into()),
                message: "server returned no body for an operation that promises one".into(),
                body: Value::Null,
            }));
        }
        Ok(serde_json::from_slice(&bytes)?)
    }

    /// Send an operation whose contract has no response body.
    pub async fn send_empty(self) -> Result<()> {
        let _ = self.execute().await?;
        Ok(())
    }

    /// Send and retain raw response bytes.
    pub async fn send_raw(self) -> Result<RawResponse> {
        let response = self.execute().await?;
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        Ok(RawResponse {
            bytes: response.bytes().await?.to_vec(),
            content_type,
        })
    }

    /// Open and decode an SSE response as a typed stream.
    pub async fn stream<T: SseDecode>(self) -> Result<SseStream<T>> {
        let generation = self.client.0.session.read().await.generation;
        match self.open_stream().await {
            Err(Error::Api(error)) if error.status == 401 => {
                if !self.client.refresh_if_generation(generation).await? {
                    return Err(error.into());
                }
                self.open_stream().await
            }
            result => result,
        }
    }

    async fn open_stream<T: SseDecode>(&self) -> Result<SseStream<T>> {
        let mut source = self
            .build()
            .await?
            .eventsource()
            .map_err(|error| Error::Sse(error.to_string()))?;
        match source.next().await {
            Some(Ok(Event::Open)) => Ok(SseStream::from_source(source)),
            Some(Ok(Event::Message(_))) => Err(Error::Sse(
                "SSE message arrived before the open event".into(),
            )),
            Some(Err(reqwest_eventsource::Error::InvalidStatusCode(_, response))) => {
                match checked(response).await {
                    Err(error) => Err(error),
                    Ok(_) => Err(Error::Sse("SSE endpoint rejected its status".into())),
                }
            }
            Some(Err(error)) => Err(Error::Sse(error.to_string())),
            None => Err(Error::Closed),
        }
    }

    async fn execute(self) -> Result<reqwest::Response> {
        let generation = self.client.0.session.read().await.generation;
        let response = self.build().await?.send().await?;
        if response.status() != reqwest::StatusCode::UNAUTHORIZED {
            return checked(response).await;
        }
        if !self.client.refresh_if_generation(generation).await? {
            return checked(response).await;
        }
        checked(self.build().await?.send().await?).await
    }

    async fn build(&self) -> Result<reqwest::RequestBuilder> {
        let mut url = format!("{}{}", self.client.0.base_url, self.path);
        if let Some(query) = &self.query {
            if !query.is_empty() {
                url.push('?');
                url.push_str(query);
            }
        }
        let mut request = self.client.0.http.request(self.method.clone(), url);
        for (name, value) in &self.client.0.headers {
            request = request.header(
                HeaderName::try_from(name.as_str())
                    .map_err(|error| Error::Configuration(error.to_string()))?,
                HeaderValue::try_from(value.as_str())
                    .map_err(|error| Error::Configuration(error.to_string()))?,
            );
        }
        if let Some(token) = self.client.0.session.read().await.access_token.clone() {
            request = request.bearer_auth(token);
        }
        if let Some(body) = &self.body {
            request = request.json(body);
        }
        Ok(request)
    }
}

impl Client {
    pub(crate) async fn refresh_if_generation(&self, observed: u64) -> Result<bool> {
        let _gate = self.0.refresh_gate.lock().await;
        let snapshot = self.0.session.read().await.clone();
        if snapshot.generation != observed {
            return Ok(true);
        }
        let (Some(refresh_token), Some(refresh_path)) =
            (snapshot.refresh_token, snapshot.refresh_path)
        else {
            return Ok(false);
        };

        // This refresh-only request deliberately bypasses RequestBuilder so a
        // 401 cannot recursively enter refresh. Refresh tokens are single-use.
        let url = format!("{}{}", self.0.base_url, refresh_path);
        let mut request = self
            .0
            .http
            .post(url)
            .json(&json!({ "refresh_token": refresh_token }));
        for (name, value) in &self.0.headers {
            request = request.header(name, value);
        }
        let response = checked(request.send().await?).await?;
        let body: Value = response.json().await?;
        let access = body
            .get("access_token")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::Configuration("refresh response omitted access_token".into()))?;
        let refresh = body
            .get("refresh_token")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let mut session = self.0.session.write().await;
        session.access_token = Some(access.to_owned());
        if refresh.is_some() {
            session.refresh_token = refresh;
        }
        session.generation = session.generation.wrapping_add(1);
        let persisted = crate::AppSession {
            access_token: access.to_owned(),
            refresh_token: session.refresh_token.clone(),
            access_token_expires_at: session.access_token_expires_at,
            user: session.user.clone(),
        };
        drop(session);
        if let Some(store) = &self.0.session_store {
            // The fresh in-memory bearer must remain usable if secure storage
            // has a transient failure; the next successful rotation retries.
            let _ = store.save(&persisted).await;
        }
        Ok(true)
    }
}

async fn checked(response: reqwest::Response) -> Result<reqwest::Response> {
    if response.status().is_success() {
        return Ok(response);
    }
    let status = response.status().as_u16();
    let bytes = response.bytes().await?;
    let body: Value = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()));
    let code = body
        .get("code")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| {
            body.get("error")
                .and_then(|error| error.get("code"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        });
    let message = body
        .get("message")
        .and_then(Value::as_str)
        .or_else(|| {
            body.get("error")
                .and_then(|error| error.get("message"))
                .and_then(Value::as_str)
        })
        .unwrap_or("request failed")
        .to_owned();
    Err(ApiError {
        status,
        code,
        message,
        body,
    }
    .into())
}
