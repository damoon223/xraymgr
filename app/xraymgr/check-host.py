#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from html import unescape
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

import os
import requests

ME_URL = "https://check-host.net/me"
IP_INFO_URL = "https://check-host.net/ip-info?host={host}"

# خروجی نهایی دقیقاً همین‌ها
OUTPUT_FIELDS = ["IP address", "ISP", "Country", "Region", "City"]

# در ip-info معمولاً ISP با "ISP / Org" می‌آید
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

PROVIDER_HDR_RE = re.compile(r"^\s*(?P<name>.+?)\s*\((?P<date>\d{2}\.\d{2}\.\d{4})\)\s*$")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ddmmyyyy_to_iso(s: str) -> Optional[str]:
    try:
        return datetime.strptime(s, "%d.%m.%Y").date().isoformat()
    except Exception:
        return None


def first_ip(text: str) -> Optional[str]:
    ip_re = re.compile(
        r"(?i)\b(?:(?:\d{1,3}\.){3}\d{1,3})\b|"
        r"\b(?:[0-9a-f]{0,4}:){2,7}[0-9a-f]{0,4}\b"
    )
    m = ip_re.search(text or "")
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
    ورودی‌های قابل قبول:
      - socks5h://user:pass@host:port
      - socks5://host:port
      - user:pass@host:port
      - host:port

    اگر scheme نداشته باشد، پیش‌فرض socks5h (remote DNS) گذاشته می‌شود.
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

    # اگر یوزرنیم/پسورد جداگانه دادید و در URL نبود، تزریق کن
    if (username or password) and not u.username and u.hostname and u.port:
        userinfo = username or ""
        if password is not None:
            userinfo = f"{userinfo}:{password}"
        s = f"{u.scheme}://{userinfo}@{u.hostname}:{u.port}"

    return s


def build_requests_proxies(proxy_url: str) -> Dict[str, str]:
    # requests برای SOCKS از dict proxies استاندارد استفاده می‌کند
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
                error_detail="Anti-bot/captcha page returned instead of expected HTML.",
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
        # وقتی requests بدون socks extra باشد معمولاً اینجا می‌افتد
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


def resolve_host_via_me(session: requests.Session, timeout: int) -> Dict[str, Any]:
    r = fetch(session, ME_URL, timeout=timeout)
    out: Dict[str, Any] = {
        "status": "error",
        "requested_url": ME_URL,
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
    out["host"] = host or first_ip(r.text or "")
    out["status"] = "ok" if out["host"] else "error"
    if out["status"] != "ok":
        out["error_type"] = "parse_failed"
        out["error_detail"] = "Could not extract host/IP from redirect URL or HTML."
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
                    val = clean_value(ln[len(label):])
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
    # اگر bs4 نصب باشد، دقیق‌تر؛ اگر نباشد fallback خطی
    try:
        from bs4 import BeautifulSoup  # type: ignore

        soup = BeautifulSoup(html or "", "html.parser")
        detected = first_ip(soup.get_text("\n", strip=True))

        providers: List[Dict[str, Any]] = []
        seen: set[Tuple[str, Optional[str]]] = set()

        for txt_node in soup.find_all(string=re.compile(r"\(\d{2}\.\d{2}\.\d{4}\)")):
            header = (txt_node or "").strip()
            m = PROVIDER_HDR_RE.match(header)
            if not m:
                continue

            provider = m.group("name").strip()
            as_of = ddmmyyyy_to_iso(m.group("date"))
            key = (provider, as_of)
            if key in seen:
                continue

            parent = getattr(txt_node, "parent", None)
            table = parent.find_next("table") if parent else None
            if not table:
                continue

            data: Dict[str, Any] = {}
            for tr in table.find_all("tr"):
                tds = tr.find_all(["td", "th"])
                if len(tds) < 2:
                    continue
                label = tds[0].get_text(" ", strip=True)
                value = clean_value(tds[1].get_text(" ", strip=True))
                data[label] = value

            providers.append({"provider": provider, "as_of": as_of, "data": data})
            seen.add(key)

        if not providers:
            return parse_ip_info_from_lines(extract_text_lines(html))

        return {"detected_ip": detected, "providers": providers}

    except ImportError:
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
                if v == "" or v.lower() == "null":
                    continue
                simplified[out_key] = v
                sources[out_key] = pname
                break

    return simplified, sources


def make_session(proxy_url: Optional[str]) -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (compatible; check-host-ipinfo-simple-socks/2.0)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
    )
    if proxy_url:
        s.proxies.update(build_requests_proxies(proxy_url))
    return s


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", help="IP/hostname. اگر ندهید از /me استخراج می‌شود.")
    ap.add_argument("--timeout", type=int, default=20)

    # SOCKS5
    ap.add_argument(
        "--socks5",
        help="پروکسی SOCKS5. مثال: socks5h://user:pass@127.0.0.1:9050 یا 127.0.0.1:9050",
    )
    ap.add_argument("--socks5-user", help="نام کاربری (اگر در URL نبود)")
    ap.add_argument("--socks5-pass", help="رمز (اگر در URL نبود)")
    ap.add_argument(
        "--local-dns",
        action="store_true",
        help="به‌جای socks5h از socks5 استفاده می‌کند (DNS لوکال).",
    )

    ap.add_argument("--with-sources", action="store_true", help="منبع هر فیلد را هم اضافه می‌کند.")
    args = ap.parse_args()

    # پروکسی را از CLI یا env بگیر
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
            out = {
                "status": "error",
                "fetched_at_utc": utc_now_iso(),
                "error_type": "invalid_proxy",
                "error_detail": str(e),
            }
            print(json.dumps(out, ensure_ascii=False, indent=2))
            return 2

    session = make_session(proxy_url)

    meta: Dict[str, Any] = {
        "fetched_at_utc": utc_now_iso(),
        "_proxy": proxy_url,
    }

    # 1) تعیین host
    if args.host:
        host = args.host.strip()
        meta["resolved_host"] = {"mode": "manual", "host": host}
    else:
        me = resolve_host_via_me(session, timeout=args.timeout)
        meta["resolved_host"] = {"mode": "me", **me}
        if me.get("status") != "ok" or not me.get("host"):
            out = {
                "status": "error",
                **meta,
                "error_type": me.get("error_type") or "me_failed",
                "error_detail": me.get("error_detail") or "Failed to resolve host via /me.",
            }
            print(json.dumps(out, ensure_ascii=False, indent=2))
            return 2
        host = me["host"]

    # 2) ip-info
    url = IP_INFO_URL.format(host=host)
    r = fetch(session, url, timeout=args.timeout)
    if not r.ok or not r.text:
        out = {
            "status": "error",
            **meta,
            "requested_url": url,
            "http_status": r.status_code,
            "final_url": r.final_url,
            "error_type": r.error_type or "ip_info_failed",
            "error_detail": r.error_detail or "Failed to fetch ip-info page.",
        }
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return 2

    ipinfo = parse_ip_info_html(r.text)
    simplified, sources = simplify_with_fallback(ipinfo)

    out: Dict[str, Any] = {"status": "ok", **meta, **simplified}
    if args.with_sources:
        out["_sources"] = sources

    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
