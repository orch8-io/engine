//! Minimal incremental Server-Sent Events parser for provider streams.
//!
//! Both streaming wire formats consumed by `llm_call` are SSE:
//! - `OpenAI` `/chat/completions` with `stream: true` — `data:` lines carrying
//!   JSON chunks, terminated by `data: [DONE]`.
//! - Anthropic `/messages` with `stream: true` — `event:` + `data:` pairs
//!   (`message_start`, `content_block_delta`, `message_delta`, …).
//!
//! The parser is byte-buffer based: network chunks may split an event (or
//! even a UTF-8 code point) anywhere, so bytes are buffered until a blank
//! line terminates an event. Splitting on `\n` byte values is UTF-8 safe —
//! `0x0A` never appears inside a multi-byte sequence.

use std::time::Duration;

use orch8_types::error::StepError;

use super::common::retryable;

/// One parsed SSE event: optional `event:` name plus the joined `data:` payload.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct SseEvent {
    /// Value of the `event:` field, when present (Anthropic names its events;
    /// `OpenAI` does not).
    pub event: Option<String>,
    /// All `data:` lines of the event, joined with `\n` per the SSE spec.
    pub data: String,
}

/// Incremental SSE parser. Feed raw body chunks with [`SseParser::push`];
/// completed events are returned as they terminate.
#[derive(Default)]
pub(super) struct SseParser {
    buf: Vec<u8>,
}

impl SseParser {
    /// Append a body chunk and drain every event completed by it.
    pub fn push(&mut self, chunk: &[u8]) -> Vec<SseEvent> {
        self.buf.extend_from_slice(chunk);
        let mut events = Vec::new();
        while let Some((end, sep_len)) = find_event_boundary(&self.buf) {
            let raw: Vec<u8> = self.buf.drain(..end + sep_len).collect();
            if let Some(event) = parse_event(&raw[..end]) {
                events.push(event);
            }
        }
        events
    }
}

/// Locate the first blank-line event terminator: `\n\n` or `\n\r\n`.
/// Returns `(payload_end, separator_len)`.
fn find_event_boundary(buf: &[u8]) -> Option<(usize, usize)> {
    let mut i = 0;
    while i + 1 < buf.len() {
        if buf[i] == b'\n' {
            if buf[i + 1] == b'\n' {
                return Some((i, 2));
            }
            if buf[i + 1] == b'\r' && i + 2 < buf.len() && buf[i + 2] == b'\n' {
                return Some((i, 3));
            }
        }
        i += 1;
    }
    None
}

/// Parse one raw event block into its `event:` / `data:` fields. Returns
/// `None` for blocks carrying neither (comments, `id:` only, keepalives).
fn parse_event(raw: &[u8]) -> Option<SseEvent> {
    let text = String::from_utf8_lossy(raw);
    let mut event = None;
    let mut data_lines: Vec<&str> = Vec::new();
    for line in text.lines() {
        // With CRLF framing the `\n\r\n` event boundary leaves the final
        // line's `\r` in the payload (no trailing `\n` for `lines()` to pair
        // it with) — strip it explicitly.
        let line = line.strip_suffix('\r').unwrap_or(line);
        if let Some(value) = line.strip_prefix("data:") {
            data_lines.push(value.strip_prefix(' ').unwrap_or(value));
        } else if let Some(value) = line.strip_prefix("event:") {
            event = Some(value.trim().to_string());
        }
        // `id:`, `retry:` and `:` comment lines are intentionally ignored.
    }
    if event.is_none() && data_lines.is_empty() {
        return None;
    }
    Some(SseEvent {
        event,
        data: data_lines.join("\n"),
    })
}

/// Default inter-chunk idle timeout: a provider stream that goes silent for
/// this long is treated as stalled and fails retryable.
pub(super) const DEFAULT_STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Resolve the inter-chunk idle timeout from `stream_idle_timeout_secs`
/// (default 30s; values of 0 fall back to the default).
pub(super) fn stream_idle_timeout(params: &serde_json::Value) -> Duration {
    params
        .get("stream_idle_timeout_secs")
        .and_then(serde_json::Value::as_u64)
        .filter(|secs| *secs > 0)
        .map_or(DEFAULT_STREAM_IDLE_TIMEOUT, Duration::from_secs)
}

/// Read the next body chunk with the inter-chunk idle timeout applied.
///
/// All mid-stream failures are **retryable**: a stall, a dropped connection,
/// or a transport error after a 200 status are transient conditions — a step
/// retry (or provider failover) re-issues the whole request.
pub(super) async fn next_chunk(
    resp: &mut reqwest::Response,
    idle_timeout: Duration,
) -> Result<Option<bytes::Bytes>, StepError> {
    match tokio::time::timeout(idle_timeout, resp.chunk()).await {
        Err(_) => Err(retryable(format!(
            "stream stalled: no data received for {idle_timeout:?}"
        ))),
        Ok(Ok(chunk)) => Ok(chunk),
        Ok(Err(e)) => Err(retryable(format!("stream transport error: {e}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(event: Option<&str>, data: &str) -> SseEvent {
        SseEvent {
            event: event.map(str::to_string),
            data: data.to_string(),
        }
    }

    #[test]
    fn parses_single_data_event() {
        let mut p = SseParser::default();
        assert_eq!(p.push(b"data: {\"a\":1}\n\n"), vec![ev(None, "{\"a\":1}")]);
    }

    #[test]
    fn parses_event_split_across_chunks() {
        let mut p = SseParser::default();
        assert!(p.push(b"data: hel").is_empty());
        assert!(p.push(b"lo\n").is_empty());
        assert_eq!(p.push(b"\n"), vec![ev(None, "hello")]);
    }

    #[test]
    fn parses_multiple_events_in_one_chunk() {
        let mut p = SseParser::default();
        let events = p.push(b"data: a\n\ndata: b\n\n");
        assert_eq!(events, vec![ev(None, "a"), ev(None, "b")]);
    }

    #[test]
    fn parses_named_events_with_crlf() {
        let mut p = SseParser::default();
        let events = p.push(b"event: message_start\r\ndata: {}\r\n\r\n");
        assert_eq!(events, vec![ev(Some("message_start"), "{}")]);
    }

    #[test]
    fn joins_multi_line_data() {
        let mut p = SseParser::default();
        assert_eq!(
            p.push(b"data: line1\ndata: line2\n\n"),
            vec![ev(None, "line1\nline2")]
        );
    }

    #[test]
    fn ignores_comments_and_ids() {
        let mut p = SseParser::default();
        assert!(p.push(b": keepalive\n\nid: 7\n\n").is_empty());
    }

    #[test]
    fn data_without_space_after_colon() {
        let mut p = SseParser::default();
        assert_eq!(p.push(b"data:[DONE]\n\n"), vec![ev(None, "[DONE]")]);
    }

    #[test]
    fn multibyte_utf8_split_across_chunks_survives() {
        let mut p = SseParser::default();
        let payload = "data: caf\u{e9}\n\n".as_bytes().to_vec();
        let (a, b) = payload.split_at(8); // splits the two-byte 'é'
        assert!(p.push(a).is_empty());
        assert_eq!(p.push(b), vec![ev(None, "caf\u{e9}")]);
    }

    #[test]
    fn idle_timeout_default_and_override() {
        assert_eq!(
            stream_idle_timeout(&serde_json::json!({})),
            DEFAULT_STREAM_IDLE_TIMEOUT
        );
        assert_eq!(
            stream_idle_timeout(&serde_json::json!({"stream_idle_timeout_secs": 5})),
            Duration::from_secs(5)
        );
        assert_eq!(
            stream_idle_timeout(&serde_json::json!({"stream_idle_timeout_secs": 0})),
            DEFAULT_STREAM_IDLE_TIMEOUT
        );
    }
}
