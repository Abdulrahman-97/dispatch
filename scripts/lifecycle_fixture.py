"""SIMULATED loopback HTTP commands/receipt failures, not ClickHouse evidence."""
import http.server
import json
import threading
import time

events = []
config = {}
offered = False
lock = threading.Lock()


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def reply(self, status, data=None):
        body = json.dumps(data).encode() if data is not None else b""
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass  # Deliberately interrupted fixture client.

    def do_GET(self):
        self.reply(200, events)

    def do_POST(self):
        global offered, config
        payload = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
        if self.path == "/setup":
            config = payload
            self.reply(204)
            return
        with lock:
            events.append({"path": self.path, "at": time.time(), "payload": payload})
        if self.path == "/remote":
            time.sleep(config.get("remote_seconds", 3))
            events.append({"path": "remote_complete", "at": time.time()})
            self.reply(204)
        elif self.path == "/internal/poll" and not offered:
            offered = True
            command = "import urllib.request; urllib.request.urlopen(urllib.request.Request('http://127.0.0.1:4999/remote',data=b'{}'),timeout=60).read()"
            self.reply(200, {"job_id": "fixture", "job_type": "dagster_run", "attempt": 1,
                "started_at": "2026-09-10T00:00:00Z", "params": {"command": [config.get("python", "python3"), "-c", command],
                "resources": {"default_slots": 1}, "execution_timeout_seconds": 60}})
        elif self.path == "/internal/result":
            count = sum(e["path"] == self.path for e in events)
            if config.get("lost_ack") and count == 1:
                events.append({"path": "receipt_committed", "at": time.time()})
                self.close_connection = True
            else:
                self.reply(204)
        elif self.path == "/internal/heartbeat" and config.get("heartbeat_outage"):
            count = sum(e["path"] == self.path for e in events)
            self.reply(503 if count < 3 else 204)
        else:
            self.reply(204)


http.server.ThreadingHTTPServer(("127.0.0.1", 4999), Handler).serve_forever()
