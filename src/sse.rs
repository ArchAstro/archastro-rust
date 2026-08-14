//! Typed Server-Sent Events support.

use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use reqwest_eventsource::{Event, EventSource};

use crate::{Error, Result};

/// Implemented by generated endpoint-specific SSE enums.
pub trait SseDecode: Sized + Send + 'static {
    /// Decode one wire event and JSON data payload.
    fn decode(event: &str, data: &str) -> Result<Self>;
}

/// One typed SSE message.
#[derive(Debug, Clone, PartialEq)]
pub struct SseEvent<T> {
    /// Event name.
    pub event: String,
    /// Last-event ID, when supplied.
    pub id: String,
    /// Typed payload.
    pub data: T,
}

/// Auto-reconnecting typed SSE stream.
pub struct SseStream<T> {
    source: EventSource,
    marker: PhantomData<T>,
}

impl<T: SseDecode> SseStream<T> {
    pub(crate) fn from_source(source: EventSource) -> Self {
        Self {
            source,
            marker: PhantomData,
        }
    }

    /// Stop reconnection and close the stream.
    pub fn close(&mut self) {
        self.source.close();
    }
}

impl<T: SseDecode + Unpin> Stream for SseStream<T> {
    type Item = Result<SseEvent<T>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match Pin::new(&mut self.source).poll_next(cx) {
                Poll::Ready(Some(Ok(Event::Open))) => continue,
                Poll::Ready(Some(Ok(Event::Message(message)))) => {
                    let decoded = T::decode(&message.event, &message.data).map(|data| SseEvent {
                        event: message.event,
                        id: message.id,
                        data,
                    });
                    return Poll::Ready(Some(decoded));
                }
                Poll::Ready(Some(Err(error))) => {
                    return Poll::Ready(Some(Err(Error::Sse(error.to_string()))));
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}
