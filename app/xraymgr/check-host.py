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
PKG_DIR = THIS.parent
if str(PKG_DIR) not in sys.path:
    sys.path.insert(0, str(PKG_DIR))

# ===== Defaults from test_settings (if present) =====
DEFAULT_ALT_CHECK_URL = "http://myserver.com"
try:
    from test_settings import TEST_ALT_CHECK_URL as DEFAULT_ALT_CHECK_URL  # type: ignore
except Exception:
    pass


CHECKHOST_ME_URL = "https://check-host.net/me"
CHECKHOST_IP_INFO_URL = "https://check-host.net/ip-info?host={host}&lang=en"

# Public IP fallback (JSON)
IPIFY_URL = "https://api.ipify.org?format=json"

# Geo fallback (no key, HTTP)
IPAPI_URL = "http://ip-api.com/json/{ip}"

OUTPUT_FIELDS = ["IP address", "ISP", "Country", "Region", "City"]

FIELD_CANDIDATES = {
    "IP address": ["IP address"],
    "ISP": ["ISP / Org", "ISP", "Organization", "Org"],
    "Country": ["Country"],
    "Region": ["Region", "Region/State", "State"],
    "City": ["City"],
}

KNOWN_LABELS = [
    "IP address",
    "Host name",
    "IP range",
    "ASN",
    "ISP / Org",
    "Country",
    "Region",
    "City",
    "Time zone",
    "Local time",
    "Postal Code",
]

PROVIDER_HDR_RE = re.compile(r"^\s*(?P<name>.+?)\s*\((?P<date>\d{2}\.\d{2}\.\d{4})\)\s*$", re.UNICODE)

IP_RE = re.compile(
    r"(?i)\b(?:(?:\d{1,3}\.){3}\d{1,3})\b|"
    r"\b(?:[0-9a-f]{0,4}:){2,7}[0-9a-f]{0,4}\b"
)

COUNTRY_CODE_RE = re.compile(r"\(([A-Z]{2})\)\s*$")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ddmmyyyy_to_iso(s: str) -> Optional[str]:
    try:
        return datetime.strptime(s, "%d.%m.%Y").date().isoformat()
    except Exception:
        return None


def first_ip(text: str) -> Optional[str]:
    m = IP_RE.search(text or "")
    return m.group(0) if m else None


def looks_like_antibot(status: int, headers: Dict[str, str], body: str) -> Tuple[bool, str]:
    h = {k.lower(): v for k, v in headers.items()}
    server = (h.get("server") or "").lower()
    body_l = (body or "").lower()
    indicators = (
        "just a moment",
        "enable javascript and cookies",
        "__cf_chl",
        "cf-chl",
        "cf-ray",
        "turnstile",
        "captcha",
        "attention required",
    )
    header_hit = ("cf-ray" in h) or ("cloudflare" in server)
    body_hit = any(x in body_l for x in indicators)
    if status in (403, 429, 503) and (header_hit or body_hit):
        return True, "captcha_or_antibot_challenge"
    if header_hit and body_hit:
        return True, "captcha_or_antibot_challenge"
    return False, ""


def normalize_socks5_url(
    raw: str,
    *,
    prefer_remote_dns: bool = True,
    username: Optional[str] = None,
    password: Optional[str] = None,
) -> str:
    """
    Accepts:
      - socks5h://user:pass@host:port
      - socks5://host:port
      - user:pass@host:port
      - host:port
    """
    s = (raw or "").strip()
    if not s:
        raise ValueError("Empty proxy value")

    if "://" not in s:
        scheme = "socks5h" if prefer_remote_dns else "socks5"
        s = f"{scheme}://{s}"

    u = urlparse(s)
    if u.scheme not in ("socks5", "socks5h"):
        raise ValueError(f"Unsupported proxy scheme: {u.scheme}")

    if not u.hostname or not u.port:
        raise ValueError("Proxy must include host:port")

    if (username or password) and not u.username:
        userinfo = username or ""
        if password is not None:
            userinfo = f"{userinfo}:{password}"
        s = f"{u.scheme}://{userinfo}@{u.hostname}:{u.port}"

    return s


def build_requests_proxies(proxy_url: str) -> Dict[str, str]:
    return {"http": proxy_url, "https": proxy_url}


@dataclass
class FetchResult:
    ok: bool
    status_code: Optional[int]
    final_url: Optional[str]
    error_type: Optional[str]
    error_detail: Optional[str]
    text: Optional[str]


def fetch(session: requests.Session, url: str, timeout: int = 20) -> FetchResult:
    try:
        r = session.get(url, timeout=timeout, allow_redirects=True)
        body = r.text or ""
        blocked, why = looks_like_antibot(r.status_code, dict(r.headers), body)
        if blocked:
            return FetchResult(
                ok=False,
                status_code=r.status_code,
                final_url=r.url,
                error_type=why,
                error_detail="Anti-bot/captcha page returned instead of expected response.",
                text=body,
            )
        if r.status_code >= 400:
            return FetchResult(
                ok=False,
                status_code=r.status_code,
                final_url=r.url,
                error_type="http_error",
                error_detail=f"HTTP {r.status_code}",
                text=body,
            )
        return FetchResult(True, r.status_code, r.url, None, None, body)
    except requests.exceptions.InvalidSchema as e:
        return FetchResult(False, None, None, "socks_missing_dependency", str(e), None)
    except requests.exceptions.ProxyError as e:
        return FetchResult(False, None, None, "proxy_error", str(e), None)
    except requests.exceptions.Timeout:
        return FetchResult(False, None, None, "connection_timeout", "Request timed out.", None)
    except requests.exceptions.SSLError as e:
        return FetchResult(False, None, None, "tls_error", str(e), None)
    except requests.exceptions.ConnectionError as e:
        return FetchResult(False, None, None, "connection_failed", str(e), None)
    except Exception as e:
        return FetchResult(False, None, None, "unexpected_error", str(e), None)


def make_session(proxy_url: Optional[str]) -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (compatible; xraymgr-check/3.0)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/json;q=0.8,*/*;q=0.7",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }
    )
    if proxy_url:
        s.proxies.update(build_requests_proxies(proxy_url))
    return s


def resolve_host_via_me(session: requests.Session, timeout: int) -> Dict[str, Any]:
    r = fetch(session, CHECKHOST_ME_URL, timeout=timeout)
    out: Dict[str, Any] = {
        "status": "error",
        "requested_url": CHECKHOST_ME_URL,
        "final_url": r.final_url,
        "http_status": r.status_code,
        "error_type": r.error_type,
        "error_detail": r.error_detail,
        "host": None,
    }
    if not r.ok or not r.final_url:
        return out

    qs = parse_qs(urlparse(r.final_url).query)
    host = (qs.get("host") or [None])[0]
    if not host:
        host = first_ip(r.text or "")

    out["host"] = host
    out["status"] = "ok" if host else "error"
    if out["status"] != "ok":
        out["error_type"] = "parse_failed"
        out["error_detail"] = "Could not extract host/IP from redirect URL or response."
    return out


def resolve_host_via_ipify(session: requests.Session, timeout: int) -> Dict[str, Any]:
    r = fetch(session, IPIFY_URL, timeout=timeout)
    out: Dict[str, Any] = {
        "status": "error",
        "requested_url": IPIFY_URL,
        "final_url": r.final_url,
        "http_status": r.status_code,
        "error_type": r.error_type,
        "error_detail": r.error_detail,
        "host": None,
    }
    if not r.ok or not r.text:
        return out
    try:
        js = json.loads(r.text)
        ip = None
        if isinstance(js, dict):
            ip = js.get("ip")
        if not ip:
            ip = first_ip(r.text)
        out["host"] = ip
        out["status"] = "ok" if ip else "error"
        if out["status"] != "ok":
            out["error_type"] = "parse_failed"
            out["error_detail"] = "Could not extract IP from ipify response."
        return out
    except Exception:
        ip = first_ip(r.text or "")
        out["host"] = ip
        out["status"] = "ok" if ip else "error"
        if out["status"] != "ok":
            out["error_type"] = "parse_failed"
            out["error_detail"] = "Could not parse ipify response."
        return out


def resolve_host_via_alt(session: requests.Session, url: str, timeout: int) -> Dict[str, Any]:
    r = fetch(session, url, timeout=timeout)
    out: Dict[str, Any] = {
        "status": "error",
        "requested_url": url,
        "final_url": r.final_url,
        "http_status": r.status_code,
        "error_type": r.error_type,
        "error_detail": r.error_detail,
        "host": None,
    }
    if not r.ok or not r.text:
        return out

    ip = None
    try:
        js = json.loads(r.text)
        if isinstance(js, dict):
            for k in ("ip", "IP", "address", "client_ip"):
                if js.get(k):
                    ip = str(js.get(k)).strip()
                    break
        if not ip:
            ip = first_ip(r.text)
    except Exception:
        ip = first_ip(r.text)

    out["host"] = ip
    out["status"] = "ok" if ip else "error"
    if out["status"] != "ok":
        out["error_type"] = "parse_failed"
        out["error_detail"] = "Could not extract IP from ALT response."
    return out


def extract_text_lines(html: str) -> List[str]:
    html2 = re.sub(r"(?is)<(script|style|noscript).*?>.*?</\1>", " ", html or "")
    text = re.sub(r"(?is)<[^>]+>", "\n", html2)
    text = unescape(text)
    return [ln.strip() for ln in text.splitlines() if ln.strip()]


def clean_value(v: str) -> Optional[str]:
    v = (v or "").strip()
    v = re.sub(r"^\bImage\b\s*", "", v).strip()
    return v if v else None


def parse_ip_info_from_lines(lines: List[str]) -> Dict[str, Any]:
    detected = first_ip(" ".join(lines))
    providers: List[Dict[str, Any]] = []
    current: Optional[Dict[str, Any]] = None

    i = 0
    while i < len(lines):
        ln = lines[i]

        m = PROVIDER_HDR_RE.match(ln)
        if m:
            if current:
                providers.append(current)
            current = {
                "provider": m.group("name").strip(),
                "as_of": ddmmyyyy_to_iso(m.group("date")),
                "data": {},
            }
            i += 1
            continue

        if current:
            if ln.lower().startswith("powered by"):
                i += 1
                continue
            if ln in ("Whois:", "BGP:", "DNS:"):
                providers.append(current)
                current = None
                i += 1
                continue

            for label in KNOWN_LABELS:
                if ln.startswith(label):
                    val = clean_value(ln[len(label) :])
                    if val is None and i + 1 < len(lines):
                        nxt = lines[i + 1]
                        if not PROVIDER_HDR_RE.match(nxt) and nxt not in ("Whois:", "BGP:", "DNS:"):
                            val = clean_value(nxt)
                            i += 1
                    current["data"][label] = val
                    break

        i += 1

    if current:
        providers.append(current)

    return {"detected_ip": detected, "providers": providers}


def parse_ip_info_html(html: str) -> Dict[str, Any]:
    # fallback (robust): linear parse from visible text
    return parse_ip_info_from_lines(extract_text_lines(html))


def simplify_with_fallback(ipinfo: Dict[str, Any]) -> Tuple[Dict[str, Optional[str]], Dict[str, Optional[str]]]:
    providers = ipinfo.get("providers") or []
    simplified: Dict[str, Optional[str]] = {k: None for k in OUTPUT_FIELDS}
    sources: Dict[str, Optional[str]] = {k: None for k in OUTPUT_FIELDS}

    for p in providers:
        pdata = p.get("data") or {}
        pname = p.get("provider")
        for out_key in OUTPUT_FIELDS:
            if simplified[out_key] is not None:
                continue
            for cand in FIELD_CANDIDATES[out_key]:
                v = pdata.get(cand)
                if v is None:
                    continue
                v = str(v).strip()
                if not v or v.lower() == "null":
                    continue
                simplified[out_key] = v
                sources[out_key] = pname
                break

    return simplified, sources


def normalize_country(v: Optional[str]) -> Optional[str]:
    if not v:
        return None
    s = str(v).strip()
    m = COUNTRY_CODE_RE.search(s)
    return m.group(1) if m else s


def normalize_isp(v: Optional[str]) -> Optional[str]:
    if not v:
        return None
    s = " ".join(str(v).strip().split())
    return s[:64] if len(s) > 64 else s


def normalize_city(v: Optional[str]) -> Optional[str]:
    if not v:
        return None
    s = " ".join(str(v).strip().split())
    return s[:48] if len(s) > 48 else s


def geo_via_checkhost(session: requests.Session, host: str, timeout: int) -> Tuple[bool, Dict[str, Optional[str]], Dict[str, Any]]:
    url = CHECKHOST_IP_INFO_URL.format(host=host)
    r = fetch(session, url, timeout=timeout)
    meta = {"requested_url": url, "final_url": r.final_url, "http_status": r.status_code, "error_type": r.error_type, "error_detail": r.error_detail}
    if not r.ok or not r.text:
        return False, {k: None for k in OUTPUT_FIELDS}, meta

    ipinfo = parse_ip_info_html(r.text)
    simplified, _sources = simplify_with_fallback(ipinfo)

    simplified["Country"] = normalize_country(simplified.get("Country"))
    simplified["City"] = normalize_city(simplified.get("City"))
    simplified["ISP"] = normalize_isp(simplified.get("ISP"))

    ok = bool(simplified.get("Country") or simplified.get("City") or simplified.get("ISP"))
    return ok, simplified, meta


def geo_via_ipapi(session: requests.Session, host: str, timeout: int) -> Tuple[bool, Dict[str, Optional[str]], Dict[str, Any]]:
    # NOTE: ip-api free endpoint is HTTP (no TLS)
    url = IPAPI_URL.format(ip=host)
    # fields to reduce payload
    url2 = url + "?fields=status,countryCode,regionName,city,isp,org,message"
    r = fetch(session, url2, timeout=timeout)
    meta = {"requested_url": url2, "final_url": r.final_url, "http_status": r.status_code, "error_type": r.error_type, "error_detail": r.error_detail}
    out = {k: None for k in OUTPUT_FIELDS}
    if not r.ok or not r.text:
        return False, out, meta
    try:
        js = json.loads(r.text)
        if not isinstance(js, dict):
            return False, out, meta
        if js.get("status") != "success":
            meta["ipapi_message"] = js.get("message")
            return False, out, meta
        out["Country"] = (js.get("countryCode") or None)
        out["Region"] = (js.get("regionName") or None)
        out["City"] = normalize_city(js.get("city"))
        out["ISP"] = normalize_isp(js.get("isp") or js.get("org"))
        return True, out, meta
    except Exception:
        return False, out, meta


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", help="IP/hostname. اگر ندهید از ALT/IPIFY/ME استخراج می‌شود.")
    ap.add_argument("--timeout", type=int, default=20)

    ap.add_argument("--alt-url", default="", help="ALT check URL (default from test_settings: TEST_ALT_CHECK_URL)")
    ap.add_argument("--no-alt", action="store_true", help="ALT check را غیرفعال می‌کند.")

    ap.add_argument(
        "--socks5",
        help="پروکسی SOCKS5. مثال: socks5h://user:pass@127.0.0.1:9050 یا 127.0.0.1:9050",
    )
    ap.add_argument("--socks5-user", help="نام کاربری (اگر در URL نبود)")
    ap.add_argument("--socks5-pass", help="رمز (اگر در URL نبود)")
    ap.add_argument("--local-dns", action="store_true", help="به‌جای socks5h از socks5 استفاده می‌کند (DNS لوکال).")

    ap.add_argument("--with-sources", action="store_true", help="متادیتا/منابع را هم اضافه می‌کند.")
    args = ap.parse_args()

    proxy_raw = args.socks5 or os.environ.get("SOCKS5_PROXY") or os.environ.get("ALL_PROXY")
    proxy_url = None
    if proxy_raw:
        try:
            proxy_url = normalize_socks5_url(
                proxy_raw,
                prefer_remote_dns=(not args.local_dns),
                username=args.socks5_user,
                password=args.socks5_pass,
            )
        except Exception as e:
            out = {"status": "error", "fetched_at_utc": utc_now_iso(), "error_type": "invalid_proxy", "error_detail": str(e)}
            print(json.dumps(out, ensure_ascii=False, indent=2))
            return 2

    session = make_session(proxy_url)

    meta: Dict[str, Any] = {"fetched_at_utc": utc_now_iso(), "_proxy": proxy_url}

    # 1) Resolve host/ip
    host = (args.host or "").strip() or None
    resolved: Dict[str, Any] = {"mode": "manual", "host": host} if host else {}

    if not host:
        alt_url = (args.alt_url or "").strip() or (os.environ.get("ALT_CHECK_URL") or "").strip() or DEFAULT_ALT_CHECK_URL
        if args.no_alt:
            alt_url = ""
        if alt_url:
            alt = resolve_host_via_alt(session, alt_url, timeout=args.timeout)
            resolved = {"mode": "alt", **alt, "alt_url": alt_url}
            host = alt.get("host")

    if not host:
        ipf = resolve_host_via_ipify(session, timeout=args.timeout)
        resolved = {"mode": "ipify", **ipf}
        host = ipf.get("host")

    if not host:
        me = resolve_host_via_me(session, timeout=args.timeout)
        resolved = {"mode": "me", **me}
        host = me.get("host")

    meta["resolved_host"] = resolved

    if not host:
        out = {
            "status": "error",
            **meta,
            "error_type": resolved.get("error_type") or "resolve_failed",
            "error_detail": resolved.get("error_detail") or "Failed to resolve IP through ALT/IPIFY/ME.",
        }
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return 2

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
        # فقط جاهای خالی را پر کن
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
