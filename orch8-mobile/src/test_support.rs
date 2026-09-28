//! Test-only HTTP control-plane mock for the node / worker tests.

use std::sync::{Arc, Mutex};

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Route function: `(method, path, json_body) -> (status, json_body)`.
pub(crate) type Route = Arc<dyn Fn(&str, &str, &Value) -> (u16, Value) + Send + Sync>;

/// Recorded request: `(path, json_body)`.
pub(crate) type Recorded = Arc<Mutex<Vec<(String, Value)>>>;

pub(crate) struct MockControlPlane {
    /// API base, e.g. `http://127.0.0.1:1234/api/v1`.
    pub base: String,
    pub requests: Recorded,
}

impl MockControlPlane {
    /// Bodies of requests whose path ends with `suffix`.
    pub fn bodies(&self, suffix: &str) -> Vec<Value> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(path, _)| path.ends_with(suffix))
            .map(|(_, body)| body.clone())
            .collect()
    }

    pub fn count(&self, suffix: &str) -> usize {
        self.bodies(suffix).len()
    }
}

/// Serve `route` on a loopback port until the runtime shuts down. Each
/// connection handles one request (`Connection: close`).
pub(crate) async fn spawn_control_plane(route: Route) -> MockControlPlane {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let requests: Recorded = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&requests);
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            let route = Arc::clone(&route);
            let recorded = Arc::clone(&recorded);
            tokio::spawn(async move {
                let Some((method, path, body)) = read_request(&mut socket).await else {
                    return;
                };
                recorded.lock().unwrap().push((path.clone(), body.clone()));
                let (status, reply) = route(&method, &path, &body);
                let reply = reply.to_string();
                let response = format!(
                    "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{reply}",
                    reply.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            });
        }
    });
    MockControlPlane {
        base: format!("http://127.0.0.1:{port}/api/v1"),
        requests,
    }
}

async fn read_request(socket: &mut tokio::net::TcpStream) -> Option<(String, String, Value)> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let header_end = loop {
        let n = socket.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos + 4;
        }
    };
    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let mut lines = head.lines();
    let mut request_line = lines.next()?.split_whitespace();
    let method = request_line.next()?.to_string();
    let path = request_line.next()?.to_string();
    let length = lines
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.trim().parse::<usize>().ok())
        .unwrap_or(0);
    while buf.len() < header_end + length {
        let n = socket.read(&mut chunk).await.ok()?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    let body = serde_json::from_slice(&buf[header_end..]).unwrap_or(Value::Null);
    Some((method, path, body))
}
