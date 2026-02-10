#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict, Optional
from urllib.parse import parse_qs, urlparse


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class Handler(BaseHTTPRequestHandler):
    server_version = "xraymgr-alt-check/1.0"

    def _client_ip(self) -> str:
        # واقعی‌ترین IP از دید TCP همین است؛ اگر پشت reverse-proxy هستی،
        # x-forwarded-for را جداگانه گزارش می‌دهیم (بدون اعتماد کامل).
        return self.client_address[0]

    def _json(self, code: int, payload: Dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _text(self, code: int, text: str) -> None:
        body = (text or "").encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        u = urlparse(self.path)
        qs = parse_qs(u.query)

        fmt = (qs.get("format") or ["json"])[0].strip().lower()
        ip = self._client_ip()
        xff = (self.headers.get("x-forwarded-for") or "").strip()
        ua = (self.headers.get("user-agent") or "").strip()

        payload: Dict[str, Any] = {
            "ok": True,
            "ip": ip,
            "ts_utc": utc_now_iso(),
            "path": u.path,
            "x_forwarded_for": xff or None,
            "user_agent": ua or None,
        }

        if fmt in ("text", "txt", "plain"):
            self._text(200, ip)
            return

        self._json(200, payload)

    def log_message(self, fmt: str, *args) -> None:
        # بدون لاگ noisy روی stdout
        return


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--listen", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=8080)
    args = ap.parse_args()

    httpd = HTTPServer((args.listen, int(args.port)), Handler)
    httpd.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
