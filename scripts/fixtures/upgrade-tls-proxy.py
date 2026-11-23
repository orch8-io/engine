"""TLS frontend for the loopback engine used by the Android upgrade test."""

from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import ssl
import sys
from threading import Lock
from urllib.parse import urlsplit


class AcknowledgementGate:
    def __init__(self):
        self.lock = Lock()
        self.discarded_ack = False

    def response_to_client(self, method, path, status, data):
        if method == "POST" and path == "/api/v1/instances" and 200 <= status < 300:
            with self.lock:
                if not self.discarded_ack:
                    self.discarded_ack = True
                    return 503, b'{"error":"fixture withheld the first submission acknowledgement"}'
        return status, data


class UpgradeServer(ThreadingHTTPServer):
    def __init__(self, backend):
        super().__init__(("127.0.0.1", 0), UpgradeProxy)
        self.backend = backend
        self.acknowledgements = AcknowledgementGate()


class UpgradeProxy(BaseHTTPRequestHandler):
    def proxy(self):
        if not self.path.startswith("/api/v1/instances"):
            self.send_error(404)
            return
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        headers = {
            name: value
            for name in ("Content-Type", "X-Api-Key", "X-Tenant-Id")
            if (value := self.headers.get(name)) is not None
        }
        backend = self.server.backend
        connection = HTTPConnection(backend.hostname, backend.port, timeout=10)
        try:
            connection.request(self.command, self.path, body=body, headers=headers)
            response = connection.getresponse()
            status, data = self.server.acknowledgements.response_to_client(
                self.command, self.path, response.status, response.read(),
            )
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        finally:
            connection.close()

    do_GET = proxy
    do_POST = proxy


if __name__ == "__main__":
    base, certificate, key, port_file = sys.argv[1:]
    backend = urlsplit(base)
    if backend.scheme != "http" or backend.hostname != "127.0.0.1":
        sys.exit("The backend must be the fixture's loopback engine")
    server = UpgradeServer(backend)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certificate, key)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    file = Path(port_file)
    temporary = file.with_suffix(".tmp")
    temporary.write_text(str(server.server_port))
    temporary.replace(file)
    server.serve_forever()
