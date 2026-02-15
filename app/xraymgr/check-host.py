#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

import requests


THIS = Path(__file__).resolve()
PKG = THIS.parent

OUTPUT_FIELDS = [
    "IP address",
    "Country",
    "Region",
    "City",
    "Latitude",
    "Longitude",
    "ISP",
]

UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def normalize_city(v: Optional[str]) -> Optional[str]:
    if not v:
        return None
    s = re.sub(r"\s+", " ", str(v)).strip()
    return s or None


def normalize_isp(v: Optional[str]) -> Optional[str]:
    if not v:
        return None
    s = re.sub(r"\s+", " ", str(v)).strip()
    return s or None


def normalize_country(v: Optional[str]) -> Optional[str]:
    if not v:
        return None
    s = re.sub(r"\s+", " ", str(v)).strip()
    return s or None


@dataclass
class FetchMeta:
    url: str
    ok: bool
    status_code: Optional[int]
    elapsed_ms: Optional[int]
    error: Optional[str]
    content_type: Optional[str]
    final_url: Optional[str]


def make_session(proxy_url: Optional[str]) -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept": "*/*"})
    if proxy_url:
        s.proxies.update({"http": proxy_url, "https": proxy_url})
    return s


def fetch_text(session: requests.Session, url: str, *, timeout: int) -> Tuple[Optional[str], FetchMeta]:
    t0 = datetime.now(timezone.utc)
    try:
        r = session.get(url, timeout=timeout, allow_redirects=True)
        dt = datetime.now(timezone.utc) - t0
        meta = FetchMeta(
            url=url,
            ok=bool(r.ok),
            status_code=int(r.status_code),
            elapsed_ms=int(dt.total_seconds() * 1000),
            error=None,
            content_type=str(r.headers.get("Content-Type") or ""),
            final_url=str(r.url),
        )
        return r.text, meta
    except Exception as e:
        dt = datetime.now(timezone.utc) - t0
        meta = FetchMeta(
            url=url,
            ok=False,
            status_code=None,
            elapsed_ms=int(dt.total_seconds() * 1000),
            error=str(e),
            content_type=None,
            final_url=None,
        )
        return None, meta


def geo_via_ipapi(session: requests.Session, ip: str, *, timeout: int) -> Tuple[bool, Dict[str, Optional[str]], Dict[str, Any]]:
    url = f"https://ipapi.co/{ip}/json/"
    text, meta = fetch_text(session, url, timeout=timeout)
    out: Dict[str, Optional[str]] = {k: None for k in OUTPUT_FIELDS}
    info: Dict[str, Any] = {"fetch": meta.__dict__}

    if not text:
        return False, out, info

    try:
        js = json.loads(text)
    except Exception as e:
        info["parse_error"] = str(e)
        info["raw"] = text[:2000]
        return False, out, info

    if isinstance(js, dict):
        out["Country"] = js.get("country_name") or js.get("country")
        out["Region"] = js.get("region")
        out["City"] = js.get("city")
        out["Latitude"] = str(js.get("latitude") or "") or None
        out["Longitude"] = str(js.get("longitude") or "") or None
        out["ISP"] = js.get("org") or js.get("asn") or js.get("network")

    ok = bool(out.get("Country") or out.get("City") or out.get("ISP"))
    return ok, out, info


def parse_checkhost_html(html: str) -> Dict[str, Optional[str]]:
    # صفحات check-host ممکن است به‌هم بریزد. این پارس ساده/بهترین‌تلاش است.
    h = unescape(html or "")
    h = re.sub(r"\r", "", h)

    def pick(label: str) -> Optional[str]:
        # نمونه‌ها: "<td>Country</td><td>Germany</td>"
        m = re.search(rf">{re.escape(label)}<.*?</t[dh]>\s*<t[dh][^>]*>\s*([^<]+)\s*<", h, flags=re.I | re.S)
        if not m:
            return None
        v = re.sub(r"\s+", " ", m.group(1)).strip()
        return v or None

    out: Dict[str, Optional[str]] = {k: None for k in OUTPUT_FIELDS}
    out["Country"] = pick("Country")
    out["Region"] = pick("Region")
    out["City"] = pick("City")
    out["ISP"] = pick("ISP") or pick("Organization") or pick("Org")
    out["Latitude"] = pick("Latitude")
    out["Longitude"] = pick("Longitude")
    return out


def geo_via_checkhost(session: requests.Session, ip: str, *, timeout: int) -> Tuple[bool, Dict[str, Optional[str]], Dict[str, Any]]:
    url = f"https://check-host.net/ip-info?host={ip}"
    text, meta = fetch_text(session, url, timeout=timeout)
    out: Dict[str, Optional[str]] = {k: None for k in OUTPUT_FIELDS}
    info: Dict[str, Any] = {"fetch": meta.__dict__}

    if not text:
        return False, out, info

    out.update(parse_checkhost_html(text))
    ok = bool(out.get("Country") or out.get("City") or out.get("ISP"))
    if not ok:
        info["raw_head"] = text[:2000]
    return ok, out, info


def resolve_via_alt(session: requests.Session, *, timeout: int, alt_url: str) -> Tuple[bool, Dict[str, Any]]:
    # ALT باید JSON برگرداند: {"ip":"x.x.x.x"} یا {"origin":"x.x.x.x"}
    text, meta = fetch_text(session, alt_url, timeout=timeout)
    if not text:
        return False, {"fetch": meta.__dict__}

    try:
        js = json.loads(text)
    except Exception as e:
        return False, {"fetch": meta.__dict__, "parse_error": str(e), "raw": text[:2000]}

    ip = None
    if isinstance(js, dict):
        ip = js.get("ip") or js.get("origin") or js.get("query")
    if ip and isinstance(ip, str):
        ip = ip.strip()
        if "," in ip:
            ip = ip.split(",")[0].strip()

    return bool(ip), {"fetch": meta.__dict__, "ip": ip}


def resolve_via_ipify(session: requests.Session, *, timeout: int) -> Tuple[bool, Dict[str, Any]]:
    return resolve_via_alt(session, timeout=timeout, alt_url="https://api.ipify.org?format=json")


def resolve_via_me(session: requests.Session, *, timeout: int) -> Tuple[bool, Dict[str, Any]]:
    return resolve_via_alt(session, timeout=timeout, alt_url="https://ipinfo.io/json")


def parse_proxy_url(raw: str) -> Optional[str]:
    s = (raw or "").strip()
    if not s:
        return None
    # اگر socks5h:// یا socks5:// باشد همان را می‌دهیم به requests via requests[socks]
    return s


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--socks5", default="", help="socks5h://user:pass@host:port")
    ap.add_argument("--timeout", type=int, default=30)
    ap.add_argument("--host", default="", help="اگر خالی باشد IP از طریق منابع خارجی resolve می‌شود")
    ap.add_argument("--alt-url", default="https://api.ipify.org?format=json")
    ap.add_argument("--with-sources", action="store_true", help="include _geo_meta with fetch details")
    args = ap.parse_args(argv)

    proxy_url = parse_proxy_url(args.socks5)

    # نیاز به requests[socks] برای socks
    if proxy_url and proxy_url.startswith("socks"):
        try:
            import socks  # noqa: F401
        except Exception:
            out = {
                "status": "error",
                "fetched_at_utc": utc_now_iso(),
                "error_type": "socks_missing_dependency",
                "error_detail": "requests[socks] (PySocks) is not installed.",
            }
            print(json.dumps(out, ensure_ascii=False, indent=2))
            return 2

    session = make_session(proxy_url)

    meta: Dict[str, Any] = {"fetched_at_utc": utc_now_iso(), "_proxy": proxy_url}

    # 1) Resolve host/ip
    host = (args.host or "").strip() or None
    resolved: Dict[str, Any] = {"mode": "manual", "host": host} if host else {}

    if not host:
        alt_url = (args.alt_url or "").strip() or "https://api.ipify.org?format=json"
        ok_alt, alt = resolve_via_alt(session, timeout=args.timeout, alt_url=alt_url)
        resolved["alt"] = alt
        host = (alt.get("ip") or "").strip() if ok_alt else None

    if not host:
        ok_ipify, ipify = resolve_via_ipify(session, timeout=args.timeout)
        resolved["ipify"] = ipify
        host = (ipify.get("ip") or "").strip() if ok_ipify else None

    if not host:
        ok_me, me = resolve_via_me(session, timeout=args.timeout)
        resolved["me"] = me
        host = (me.get("ip") or "").strip() if ok_me else None

    if not host:
        out = {
            "status": "error",
            "fetched_at_utc": utc_now_iso(),
            "error_type": "resolve_failed",
            "error_detail": resolved.get("error_detail") or "Failed to resolve IP through ALT/IPIFY/ME.",
        }
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return 2

    meta["resolved_host"] = resolved

    # 2) Geo/IP-Info (best-effort)
    simplified: Dict[str, Optional[str]] = {k: None for k in OUTPUT_FIELDS}
    geo_meta: Dict[str, Any] = {}

    ok_ch, ch_geo, ch_meta = geo_via_checkhost(session, host, timeout=args.timeout)
    simplified.update(ch_geo)
    geo_meta["checkhost"] = ch_meta

    need_fallback = (not ok_ch) or (not simplified.get("Country")) or (not simplified.get("City")) or (not simplified.get("ISP"))

    if need_fallback:
        ok_ipapi, ipapi_geo, ipapi_meta = geo_via_ipapi(session, host, timeout=args.timeout)
        geo_meta["ipapi"] = ipapi_meta
        for k in ("Country", "Region", "City", "ISP"):
            if not simplified.get(k) and ipapi_geo.get(k):
                simplified[k] = ipapi_geo.get(k)

    simplified["IP address"] = host
    simplified["Country"] = normalize_country(simplified.get("Country"))
    simplified["City"] = normalize_city(simplified.get("City"))
    simplified["ISP"] = normalize_isp(simplified.get("ISP"))

    out: Dict[str, Any] = {"status": "ok", **meta, **simplified}

    if args.with_sources:
        out["_geo_meta"] = geo_meta

    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
