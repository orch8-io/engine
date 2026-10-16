//! Scriptable loopback HTTP/1.1 mock for handler and loop tests (T1 HX-5,
//! T3-HAR-05's mock receiver), replacing the hand-rolled per-file axum /
//! `TcpListener` mocks.
//!
//! ```ignore
//! let mock = MockHttp::start(vec![
//!     Reply::status(503).header("Retry-After", "120"),
//!     Reply::redirect("http://169.254.169.254/latest"),
//!     Reply::Hang,
//! ]).await;                                   // then the fallback: 200 `{}`
//! let url = mock.url("/hook");                // http://127.0.0.1:<port>/hook
//! mock.mark_safe("/hook").await;              // let the SSRF pre-check pass
//! ... run the code under test ...
//! assert_eq!(mock.requests()[0].method, "POST");
//! assert_eq!(mock.connection_count(), 1);
//! ```
//!
//! Every response carries `Connection: close`, so each request is one TCP
//! connection and [`MockHttp::connection_count`] is exact. Dropping the mock
//! stops the listener and every open connection (including hung ones).

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;

/// Largest request head the mock accepts.
const MAX_HEAD_BYTES: usize = 64 * 1024;

/// One scripted response.
#[derive(Debug, Clone)]
pub enum Reply {
    /// A complete response.
    Respond {
        status: u16,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    },
    /// Read the request, then never answer (until the mock is dropped).
    Hang,
    /// Read the request, then close the connection without a response.
    Close,
    /// Wait, then send the response.
    Delayed(Duration, Box<Reply>),
}

impl Reply {
    /// `status` with an empty body.
    #[must_use]
    pub fn status(status: u16) -> Self {
        Self::Respond {
            status,
            headers: Vec::new(),
            body: Vec::new(),
        }
    }

    /// `200` with a JSON body.
    #[must_use]
    pub fn ok_json(body: &serde_json::Value) -> Self {
        Self::status(200)
            .header("Content-Type", "application/json")
            .body(body.to_string())
    }

    /// `302` to `location`.
    #[must_use]
    pub fn redirect(location: &str) -> Self {
        Self::status(302).header("Location", location)
    }

    /// Add a response header (no-op on `Hang` / `Close`).
    #[must_use]
    pub fn header(mut self, name: &str, value: &str) -> Self {
        if let Self::Respond { headers, .. } = &mut self {
            headers.push((name.to_owned(), value.to_owned()));
        }
        self
    }

    /// Set the response body (no-op on `Hang` / `Close`).
    #[must_use]
    pub fn body(mut self, content: impl Into<Vec<u8>>) -> Self {
        if let Self::Respond { body, .. } = &mut self {
            *body = content.into();
        }
        self
    }

    /// Send this reply after `delay` (tokio time).
    #[must_use]
    pub fn after(self, delay: Duration) -> Self {
        Self::Delayed(delay, Box::new(self))
    }
}

/// A request the mock received.
#[derive(Debug, Clone)]
pub struct Recorded {
    pub method: String,
    /// Path plus query string, as sent.
    pub path: String,
    /// Header names lower-cased, in arrival order.
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Recorded {
    /// First header named `name` (case-insensitive).
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        let name = name.to_ascii_lowercase();
        self.headers
            .iter()
            .find(|(k, _)| *k == name)
            .map(|(_, v)| v.as_str())
    }

    /// The body as UTF-8 (lossy).
    #[must_use]
    pub fn body_text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }

    /// The body parsed as JSON. Panics if it is not JSON.
    #[must_use]
    pub fn body_json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).expect("request body is JSON")
    }
}

#[derive(Default)]
struct State {
    script: VecDeque<Reply>,
    fallback: Option<Reply>,
    requests: Vec<Recorded>,
    connections: u64,
    in_flight: u32,
    max_in_flight: u32,
}

/// A scripted HTTP server on `127.0.0.1:<ephemeral>`. See the module docs.
pub struct MockHttp {
    addr: SocketAddr,
    state: Arc<Mutex<State>>,
    server: tokio::task::JoinHandle<()>,
}

impl MockHttp {
    /// Serve `script` in order, then `200 {}` for every later request.
    pub async fn start(script: Vec<Reply>) -> Self {
        Self::start_with_fallback(script, Reply::ok_json(&serde_json::json!({}))).await
    }

    /// Serve `reply` to every request.
    pub async fn always(reply: Reply) -> Self {
        Self::start_with_fallback(Vec::new(), reply).await
    }

    /// Serve `script` in order, then `fallback` for every later request.
    pub async fn start_with_fallback(script: Vec<Reply>, fallback: Reply) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock listener");
        let addr = listener.local_addr().expect("mock listener address");
        let state = Arc::new(Mutex::new(State {
            script: script.into(),
            fallback: Some(fallback),
            ..State::default()
        }));
        let server_state = Arc::clone(&state);
        let server = tokio::spawn(async move {
            // Connection tasks live in this set, so aborting the server task
            // (on drop) also tears down hung connections.
            let mut connections = JoinSet::new();
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    continue;
                };
                lock(&server_state).connections += 1;
                connections.spawn(serve(stream, Arc::clone(&server_state)));
                while connections.try_join_next().is_some() {}
            }
        });
        Self {
            addr,
            state,
            server,
        }
    }

    /// `127.0.0.1:<port>`.
    #[must_use]
    pub const fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// `http://127.0.0.1:<port>` + `path`.
    #[must_use]
    pub fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }

    /// Seed the SSRF pre-check cache so outbound handlers may call
    /// [`Self::url`]`(path)` although it is loopback; returns that URL. Only
    /// this exact URL string is affected (the guard stays on for every other
    /// URL, including other paths of this mock).
    pub async fn mark_safe(&self, path: &str) -> String {
        let url = self.url(path);
        mark_url_safe(&url).await;
        url
    }

    /// Append replies to the script.
    pub fn push(&self, reply: Reply) {
        lock(&self.state).script.push_back(reply);
    }

    /// Every request received so far, in order.
    #[must_use]
    pub fn requests(&self) -> Vec<Recorded> {
        lock(&self.state).requests.clone()
    }

    /// Requests received so far.
    #[must_use]
    pub fn request_count(&self) -> usize {
        lock(&self.state).requests.len()
    }

    /// TCP connections accepted so far (also counts connections that never
    /// sent a complete request).
    #[must_use]
    pub fn connection_count(&self) -> u64 {
        lock(&self.state).connections
    }

    /// Highest number of requests in flight at once.
    #[must_use]
    pub fn max_concurrent(&self) -> u32 {
        lock(&self.state).max_in_flight
    }
}

impl Drop for MockHttp {
    fn drop(&mut self) {
        self.server.abort();
    }
}

/// Seed the SSRF pre-check cache for exactly `url` (as if DNS had said
/// "public" at check time). The connect-time resolver and the redirect
/// guard still apply.
pub async fn mark_url_safe(url: &str) {
    crate::handlers::builtin::mark_url_safe_for_test(url).await;
}

fn lock(state: &Mutex<State>) -> MutexGuard<'_, State> {
    state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

async fn serve(mut stream: TcpStream, state: Arc<Mutex<State>>) {
    let Some(request) = read_request(&mut stream).await else {
        return;
    };
    let reply = {
        let mut st = lock(&state);
        st.requests.push(request);
        st.in_flight += 1;
        st.max_in_flight = st.max_in_flight.max(st.in_flight);
        st.script
            .pop_front()
            .or_else(|| st.fallback.clone())
            .unwrap_or_else(|| Reply::status(200))
    };
    write_reply(&mut stream, reply).await;
    lock(&state).in_flight -= 1;
}

async fn write_reply(stream: &mut TcpStream, mut reply: Reply) {
    while let Reply::Delayed(delay, inner) = reply {
        tokio::time::sleep(delay).await;
        reply = *inner;
    }
    match reply {
        Reply::Respond {
            status,
            headers,
            body,
        } => {
            use std::fmt::Write as _;
            let mut head = format!("HTTP/1.1 {status} {}\r\n", reason(status));
            for (name, value) in &headers {
                let _ = write!(head, "{name}: {value}\r\n");
            }
            let _ = write!(
                head,
                "Content-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(head.as_bytes()).await;
            let _ = stream.write_all(&body).await;
            let _ = stream.shutdown().await;
        }
        Reply::Hang => std::future::pending::<()>().await,
        Reply::Close => {}
        Reply::Delayed(..) => unreachable!("unwrapped above"),
    }
}

const fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        301 => "Moved Permanently",
        302 => "Found",
        307 => "Temporary Redirect",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        _ => "Status",
    }
}

/// Read one request: head up to `\r\n\r\n`, then a `Content-Length` or
/// chunked body. `None` if the peer closed early or sent garbage.
async fn read_request(stream: &mut TcpStream) -> Option<Recorded> {
    let mut buf = Vec::with_capacity(1024);
    let head_end = loop {
        if let Some(pos) = find(&buf, b"\r\n\r\n") {
            break pos;
        }
        if buf.len() > MAX_HEAD_BYTES {
            return None;
        }
        let mut chunk = [0u8; 4096];
        let n = stream.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
    let mut lines = head.split("\r\n");
    let mut request_line = lines.next()?.split(' ');
    let method = request_line.next()?.to_owned();
    let path = request_line.next()?.to_owned();
    let headers: Vec<(String, String)> = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_owned()))
        .collect();
    let header = |name: &str| {
        headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
    };
    let mut body = buf[head_end + 4..].to_vec();
    if let Some(len) = header("content-length").and_then(|v| v.parse::<usize>().ok()) {
        while body.len() < len {
            let mut chunk = [0u8; 4096];
            let n = stream.read(&mut chunk).await.ok()?;
            if n == 0 {
                break;
            }
            body.extend_from_slice(&chunk[..n]);
        }
        body.truncate(len);
    } else if header("transfer-encoding").is_some_and(|v| v.eq_ignore_ascii_case("chunked")) {
        while find(&body, b"0\r\n\r\n").is_none() {
            let mut chunk = [0u8; 4096];
            let n = stream.read(&mut chunk).await.ok()?;
            if n == 0 {
                break;
            }
            body.extend_from_slice(&chunk[..n]);
        }
        body = dechunk(&body);
    }
    Some(Recorded {
        method,
        path,
        headers,
        body,
    })
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn dechunk(raw: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut rest = raw;
    while let Some(line_end) = find(rest, b"\r\n") {
        let size_text = String::from_utf8_lossy(&rest[..line_end]);
        let Ok(size) = usize::from_str_radix(size_text.trim(), 16) else {
            break;
        };
        if size == 0 {
            break;
        }
        let start = line_end + 2;
        let Some(chunk) = rest.get(start..start + size) else {
            break;
        };
        out.extend_from_slice(chunk);
        rest = rest.get(start + size + 2..).unwrap_or_default();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{MockHttp, Reply};

    fn client() -> reqwest::Client {
        reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn serves_the_script_in_order_then_the_fallback() {
        let mock = MockHttp::start(vec![Reply::status(503), Reply::status(201)]).await;

        let mut statuses = Vec::new();
        for _ in 0..3 {
            statuses.push(
                client()
                    .get(mock.url("/x"))
                    .send()
                    .await
                    .unwrap()
                    .status()
                    .as_u16(),
            );
        }

        assert_eq!(statuses, [503, 201, 200]);
    }

    #[tokio::test]
    async fn records_method_path_headers_and_body() {
        let mock = MockHttp::start(vec![]).await;

        client()
            .post(mock.url("/hook?a=1"))
            .header("X-Sig", "abc")
            .body(r#"{"k":1}"#)
            .send()
            .await
            .unwrap();

        let req = &mock.requests()[0];
        assert_eq!(req.method, "POST");
        assert_eq!(req.path, "/hook?a=1");
        assert_eq!(req.header("x-sig"), Some("abc"));
        assert_eq!(req.body_json()["k"], 1);
    }

    #[tokio::test]
    async fn sends_scripted_response_headers() {
        let mock = MockHttp::start(vec![Reply::redirect("http://example.invalid/next")]).await;

        let resp = client().get(mock.url("/")).send().await.unwrap();

        assert_eq!(resp.status().as_u16(), 302);
        assert_eq!(
            resp.headers().get("location").unwrap(),
            "http://example.invalid/next"
        );
    }

    #[tokio::test]
    async fn hang_never_answers() {
        let mock = MockHttp::start(vec![Reply::Hang]).await;

        let result = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            client().get(mock.url("/")).send(),
        )
        .await;

        assert!(result.is_err(), "a hung reply must not complete");
        assert_eq!(mock.request_count(), 1);
    }

    #[tokio::test]
    async fn close_drops_the_connection_without_a_response() {
        let mock = MockHttp::start(vec![Reply::Close]).await;

        let result = client().get(mock.url("/")).send().await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn counts_one_connection_per_request() {
        let mock = MockHttp::start(vec![]).await;

        client().get(mock.url("/a")).send().await.unwrap();
        client().get(mock.url("/b")).send().await.unwrap();

        assert_eq!(mock.connection_count(), 2);
        assert_eq!(mock.request_count(), 2);
    }
}
