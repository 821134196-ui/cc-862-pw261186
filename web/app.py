"""Stdlib HTTP server: JSON state API + control endpoints + static dashboard."""
from __future__ import annotations

import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .engine import DashboardEngine

WEB_DIR = os.path.dirname(os.path.abspath(__file__))

ENGINE: DashboardEngine | None = None


class Handler(BaseHTTPRequestHandler):
    server_version = "DiscoverySim/1.0"

    def log_message(self, fmt, *args):  # quiet; audit trail lives in the UI
        return

    # ---------------------------------------------------------------- helpers
    def _send_json(self, payload, status: int = 200) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_file(self, rel: str, content_type: str) -> None:
        path = os.path.join(WEB_DIR, rel)
        if not os.path.isfile(path):
            self.send_error(404)
            return
        with open(path, "rb") as fh:
            body = fh.read()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        if not length:
            return {}
        raw = self.rfile.read(length)
        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            return {}

    # ------------------------------------------------------------------ GET
    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        route = parsed.path
        if route in ("/", "/index.html"):
            self._send_file("index.html", "text/html; charset=utf-8")
        elif route == "/app.js":
            self._send_file("app.js", "application/javascript; charset=utf-8")
        elif route == "/styles.css":
            self._send_file("styles.css", "text/css; charset=utf-8")
        elif route == "/api/state":
            self._send_json(ENGINE.state())
        else:
            self.send_error(404)

    # ----------------------------------------------------------------- POST
    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        route = parsed.path
        data = self._read_json()
        try:
            if route == "/api/register":
                ENGINE.register(
                    data.get("name", "node-x"),
                    data.get("address", "127.0.0.1"),
                    data.get("zone", "az-1"),
                )
            elif route == "/api/deregister":
                ENGINE.deregister(data["name"])
            elif route == "/api/compact":
                watermark = ENGINE.compact(int(data.get("retain", 6)))
                self._send_json({"ok": True, "watermark": watermark})
                return
            elif route == "/api/client/pause":
                ENGINE.pause_client(data["name"])
            elif route == "/api/client/resume":
                ENGINE.resume_client(data["name"])
            elif route == "/api/client/disconnect":
                ENGINE.disconnect_client(data["name"])
            elif route == "/api/client/reconnect":
                ENGINE.reconnect_client(data["name"])
            elif route == "/api/client/delay":
                ENGINE.set_delay(data["name"], float(data.get("delay", 0.0)))
            elif route == "/api/churn":
                enabled = ENGINE.set_churn(bool(data.get("enabled", False)))
                self._send_json({"ok": True, "churn_enabled": enabled})
                return
            else:
                self.send_error(404)
                return
            self._send_json({"ok": True})
        except KeyError as exc:
            self._send_json({"ok": False, "error": f"missing field {exc}"}, 400)
        except Exception as exc:  # surface engine errors to the UI
            self._send_json({"ok": False, "error": repr(exc)}, 500)


def serve(host: str = "127.0.0.1", port: int = 8000) -> None:
    global ENGINE
    ENGINE = DashboardEngine()
    ENGINE.start()
    httpd = ThreadingHTTPServer((host, port), Handler)
    print(f"Dashboard: http://{host}:{port}  (Ctrl-C to stop)")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nshutting down…")
    finally:
        httpd.server_close()
        ENGINE.shutdown()
        ENGINE.thread.join(timeout=2)
