"""A tiny fake stock-movements REST API used by the demo and the tests (stdlib only).

  GET /api/stock_movements?page=1&page_size=100&since=2026-01-01T00:00:00
  Authorization: Bearer <DEMO_API_TOKEN>      (default token: demo-token)

Response: {"data": {"items": [...]}, "meta": {"page": 1, "total": 600}}
The data is deterministic. A few rows are deliberately dirty so data quality has something to catch.
"""

from __future__ import annotations

import datetime as dt
import json
import random
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

TOKEN = "demo-token"


def make_movements(n: int = 600, seed: int = 7, product_ids: list[str] | None = None) -> list[dict]:
    rng = random.Random(seed)
    products = product_ids or [f"PROD-{1001 + i}" for i in range(78)]
    now = dt.datetime.now().replace(microsecond=0)
    rows = []
    for i in range(1, n + 1):
        moved = now - dt.timedelta(minutes=rng.randint(0, 60 * 24 * 60))
        kind = rng.choices(["IN", "OUT", "ADJUST"], weights=[3, 6, 1])[0]
        qty = rng.randint(1, 60)
        rows.append(
            {
                "movement_id": i,
                "product_id": rng.choice(products),
                "movement_type": kind,
                "quantity": -qty if kind == "OUT" else qty,
                "warehouse": {"code": rng.choice(["BKK-1", "BKK-2", "CNX-1"])},
                "moved_at": moved.isoformat(sep=" "),
                "updated_at": moved.isoformat(sep=" "),
            }
        )
    if n >= 202:  # dirty data so the quality checks have something to catch
        for bad in (13, 77, 201):
            rows[bad]["movement_type"] = "UNKNOWN"
        rows[150]["quantity"] = None
    rows.sort(key=lambda r: (r["updated_at"], r["movement_id"]))
    return rows


class _Handler(BaseHTTPRequestHandler):
    data: list[dict] = []
    token: str = TOKEN
    requests_seen: list[str] = []

    def log_message(self, fmt: str, *args) -> None:  # keep the console quiet
        pass

    def _send(self, code: int, payload: dict) -> None:
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        url = urlparse(self.path)
        self.requests_seen.append(self.path)
        if url.path != "/api/stock_movements":
            return self._send(404, {"error": "not found"})
        if self.headers.get("Authorization") != f"Bearer {self.token}":
            return self._send(401, {"error": "unauthorized"})
        qs = parse_qs(url.query)
        page = int(qs.get("page", ["1"])[0])
        size = min(int(qs.get("page_size", ["100"])[0]), 500)
        since = qs.get("since", [""])[0].replace("T", " ")
        rows = [r for r in self.data if r["updated_at"] > since] if since else self.data
        items = rows[(page - 1) * size : page * size]
        self._send(200, {"data": {"items": items}, "meta": {"page": page, "total": len(rows)}})


def start_background(data: list[dict] | None = None, port: int = 0, token: str = TOKEN) -> tuple[ThreadingHTTPServer, str]:
    """Start the server in a daemon thread (used by tests). Returns (server, base_url)."""
    handler = type("Handler", (_Handler,), {"data": data if data is not None else make_movements(), "token": token, "requests_seen": []})
    server = ThreadingHTTPServer(("127.0.0.1", port), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, f"http://127.0.0.1:{server.server_address[1]}"


def serve(host: str = "127.0.0.1", port: int = 8000) -> None:
    handler = type("Handler", (_Handler,), {"data": make_movements(), "requests_seen": []})
    server = ThreadingHTTPServer((host, port), handler)
    print(f"mock API on http://{host}:{port}/api/stock_movements  (Bearer {TOKEN}) - Ctrl+C to stop")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
