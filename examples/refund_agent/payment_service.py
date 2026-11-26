#!/usr/bin/env python3
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Mock payment + email provider used by the refund-agent demo.

It is deliberately strict about idempotency: every request must carry an
``Idempotency-Key`` header, and a repeated key returns the original response
without applying the effect again. Every applied effect and every
deduplicated retry is logged to stdout, so the demo can show exactly how many
times a customer was actually charged.

Usage:
    python payment_service.py --port 9099
"""

from __future__ import annotations

import argparse
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Tuple


class Ledger:
    """In-memory effect ledger keyed by idempotency key, per endpoint."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._applied: Dict[str, Dict[str, Any]] = {}
        self._refunds: list[Dict[str, Any]] = []
        self._emails: list[Dict[str, Any]] = []
        self.deduplicated = 0

    def apply(
        self, endpoint: str, key: str, body: Dict[str, Any]
    ) -> Tuple[Dict[str, Any], bool]:
        with self._lock:
            composite = f"{endpoint}:{key}"
            if composite in self._applied:
                self.deduplicated += 1
                return self._applied[composite], True
            if endpoint == "refund":
                record = {
                    "refund_id": f"re_{len(self._refunds) + 1:04d}",
                    "order": body.get("order"),
                    "amount_cents": body.get("amount_cents"),
                    "currency": body.get("currency", "EUR"),
                    "status": "refunded",
                }
                self._refunds.append(record)
            else:
                record = {
                    "message_id": f"msg_{len(self._emails) + 1:04d}",
                    "to": body.get("to"),
                    "order": body.get("order"),
                    "status": "sent",
                }
                self._emails.append(record)
            self._applied[composite] = record
            return record, False

    def stats(self, order: str | None = None) -> Dict[str, Any]:
        with self._lock:
            refunds = [
                r for r in self._refunds if order is None or r.get("order") == order
            ]
            emails = [
                e for e in self._emails if order is None or e.get("order") == order
            ]
            return {
                "orders_charged": len(refunds),
                "refunds": refunds,
                "emails": emails,
                "deduplicated_retries": self.deduplicated,
            }


LEDGER = Ledger()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _json(self, status: int, payload: Dict[str, Any]) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        if self.path.startswith("/stats"):
            order = None
            if "?order=" in self.path:
                order = self.path.split("?order=", 1)[1]
            self._json(200, LEDGER.stats(order))
        elif self.path == "/healthz":
            self._json(200, {"status": "ok"})
        else:
            self._json(404, {"error": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        endpoint = {"/refund": "refund", "/email": "email"}.get(self.path)
        if endpoint is None:
            self._json(404, {"error": "not found"})
            return
        key = self.headers.get("Idempotency-Key")
        if not key:
            self._json(400, {"error": "Idempotency-Key header is required"})
            return
        length = int(self.headers.get("Content-Length", "0"))
        try:
            body = json.loads(self.rfile.read(length) or b"{}")
        except json.JSONDecodeError:
            self._json(400, {"error": "invalid json"})
            return

        record, was_duplicate = LEDGER.apply(endpoint, key, body)
        if was_duplicate:
            print(f"[provider] {endpoint} deduplicated key={key} (no new effect)")
        else:
            amount = record.get("amount_cents")
            detail = (
                f"€{amount / 100:.2f}" if amount is not None else record.get("to")
            )
            print(f"[provider] {endpoint} APPLIED key={key} {detail}")
        self._json(200, {**record, "deduplicated": was_duplicate})

    def log_message(self, fmt: str, *args: Any) -> None:  # silence default access log
        return


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=9099)
    args = parser.parse_args()
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    print(f"[provider] payment/email service on http://127.0.0.1:{args.port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
