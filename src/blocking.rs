//! Blocking bridge for generated `_blocking` methods.

use std::future::Future;

use crate::{Error, Result};

/// Execute one SDK future from synchronous code.
pub fn block_on<T>(future: impl Future<Output = Result<T>>) -> Result<T> {
    if tokio::runtime::Handle::try_current().is_ok() {
        return Err(Error::Configuration(
            "blocking SDK methods cannot run inside an async Tokio runtime".into(),
        ));
    }
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| Error::Configuration(error.to_string()))?
        .block_on(future)
}
