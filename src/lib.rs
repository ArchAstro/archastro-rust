//! Official, generated-first Rust SDK for ArchAstro.
//!
//! [`Client`] is asynchronous and runtime-agnostic at its public boundary.
//! Enable the default `blocking` feature for `_blocking` resource methods.

mod channel;
mod client;
mod error;
mod http;
mod session;
pub mod sse;

#[cfg(feature = "blocking")]
pub mod blocking;
/// Generated API models, resources, authentication, and channel facades.
pub mod generated;

pub use channel::{
    Channel, ChannelEventStream, ChannelState, Socket, SocketBuilder, SocketEvent,
    SocketEventStream,
};
pub use client::{Client, ClientBuilder};
pub use error::{ApiError, ChannelError, Error, Result};
pub use http::{RawResponse, RequestBuilder};
pub use session::{AppSession, SessionStore};

/// Percent-encode one path segment without changing path separators.
pub fn encode_path(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}
