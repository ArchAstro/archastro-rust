# ArchAstro Rust SDK

Official, generated-first Rust client for ArchAstro: typed HTTP resources,
automatic single-flight token refresh, SSE streams, and Phoenix channels.

```toml
[dependencies]
archastro = "0.1"
```

```rust,no_run
use archastro::Client;

#[tokio::main]
async fn main() -> archastro::Result<()> {
    let client = Client::builder()
        .secret_key(std::env::var("ARCHASTRO_SECRET_KEY").unwrap())
        .build()?;

    let status = client.v1().status().ping().await?;
    println!("{status:?}");
    Ok(())
}
```

1. Async API: every generated HTTP call is `async` and uses a cloneable,
   pooled `reqwest::Client`.
2. Blocking API: default `blocking` feature adds a `_blocking` variant to
   non-streaming methods. Do not call it from inside a Tokio runtime.
3. SSE: streaming endpoints return `SseStream<EventEnum>`, implementing
   `futures_core::Stream` with reconnection and last-event-ID support.
4. Channels: `client.socket().await?.connect().await?` opens a Phoenix v2
   socket; generated facades provide typed join responses, messages, and push
   streams. The runtime reconnects, rejoins, verifies heartbeats, and buffers
   pushes while a desired channel is reconnecting.
5. Auth: secret keys, publishable keys, bearer/system tokens, and
   `Client::with_credentials` are supported. Concurrent 401s share one
   generation-fenced refresh because refresh tokens are single-use.
6. App sessions: configure a `SessionStore`, then use `restore_session`,
   `install_app_session`, and `sign_out`. Refresh-token rotations are written
   back after the in-memory bearer is updated.

## Development

```bash
npm ci
./scripts/regenerate_sdk.sh --local ../archastro-openapi
cargo fmt --all -- --check
cargo clippy --all-features --all-targets -- -D warnings
cargo test --all-features
cargo test --all-features --test generated_rest_contract -- --ignored --test-threads=1
cargo test --all-features --test generated_stream_contract -- --ignored --test-threads=1
cargo test --all-features --test generated_channel_contract -- --ignored --test-threads=1
cargo test --all-features --test sse_runtime_contract -- --ignored --test-threads=1
cargo test --all-features --test channel_runtime_contract -- --ignored --test-threads=1
```

Generated files carry a content hash and must only be changed through
`@archastro/sdk-generator`.
