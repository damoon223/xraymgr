#!/usr/bin/env python3
import base64
import hashlib
import json
import re
from urllib.parse import urlparse, parse_qs, unquote

# ------------------ Public API ------------------ #

def canonical_hash(raw: str) -> str:
    """
    ورودی: رشته‌ی کانفیگ (می‌تواند لینک vmess/vless/trojan/ss یا JSON کامل Xray/V2Ray باشد).
    خروجی: هش هگز SHA-256 پس از نرمال‌سازی و فلت‌کردن.
    """
    raw = (raw or "").strip()
    if not raw:
        return sha256_hex(b"")

    # اگر لینک URI است، بر اساس schema پردازش کن
    lower = raw.lower()
    try:
        if lower.startswith("vmess://"):
            norm = normalize_vmess_uri(raw)
        elif lower.startswith("vless://"):
            norm = normalize_vless_uri(raw)
        elif lower.startswith("trojan://"):
            norm = normalize_trojan_uri(raw)
        elif lower.startswith("ss://"):
            norm = normalize_ss_uri(raw)
        else:
            # فرض بر JSON کامل Xray/V2Ray
            obj = json.loads(raw)
            norm = normalize_xray_json(obj)
    except Exception:
        # در صورت خطا، از trimmed raw هش می‌گیریم
        return sha256_hex(raw.encode("utf-8"))

    flat = flatten_to_text(norm)
    flat = flat.lower()
    return sha256_hex(flat.encode("utf-8"))

# ------------------ Protocol normalizers ------------------ #

def normalize_vmess_uri(uri: str) -> dict:
    # vmess://<base64(json)>
    b64 = uri[len("vmess://"):]
    padded = b64 + "=" * (-len(b64) % 4)
    data = base64.urlsafe_b64decode(padded.encode("utf-8"))
    obj = json.loads(data.decode("utf-8"))
    return {
        "protocol": "vmess",
        "add": obj.get("add"),
        "port": str(obj.get("port") or ""),
        "id": obj.get("id"),
        "aid": str(obj.get("aid") or obj.get("alterId") or ""),
        "scy": obj.get("scy") or "",  # security
        "net": obj.get("net"),        # ws/grpc/tcp/kcp/quic/http
        "type": obj.get("type") or "",# header type
        "host": obj.get("host") or obj.get("sni") or "",
        "path": obj.get("path") or "",
        "tls": obj.get("tls") or "",
        "sni": obj.get("sni") or "",
        "alpn": _to_list_str(obj.get("alpn")),
        "fp": obj.get("fp") or "",
    }

def normalize_vless_uri(uri: str) -> dict:
    # vless://<uuid>[@host:port]?params
    u = urlparse(uri)
    user = u.username or ""
    host = u.hostname or ""
    port = u.port or ""
    q = parse_qs(u.query)

    return {
        "protocol": "vless",
        "id": user,  # UUID
        "host": host,
        "port": str(port),
        "flow": _first(q, "flow"),
        "security": _first(q, "security") or _first(q, "encryption") or "",
        "sni": _first(q, "sni"),
        "alpn": _to_list_str(_first_list(q, "alpn")),
        "fp": _first(q, "fp"),
        "type": _first(q, "type") or _first(q, "transport") or "",
        "path": _first(q, "path") or "",
        "mode": _first(q, "mode") or "",  # grpc mode
        "serviceName": _first(q, "serviceName") or _first(q, "servicename") or "",
        "authority": _first(q, "authority") or "",
        "pbk": _first(q, "pbk") or _first(q, "publickey") or "",
        "sid": _first(q, "sid") or _first(q, "shortid") or "",
        "spx": _first(q, "spx") or _first(q, "spiderx") or "",
    }

def normalize_trojan_uri(uri: str) -> dict:
    # trojan://password@host:port?...
    u = urlparse(uri)
    pwd = u.username or ""
    host = u.hostname or ""
    port = u.port or ""
    q = parse_qs(u.query)
    return {
        "protocol": "trojan",
        "password": pwd,
        "host": host,
        "port": str(port),
        "security": _first(q, "security") or "",
        "sni": _first(q, "sni"),
        "alpn": _to_list_str(_first_list(q, "alpn")),
        "type": _first(q, "type") or "",
        "path": _first(q, "path") or "",
        "host_header": _first(q, "host") or "",
        "serviceName": _first(q, "serviceName") or "",
        "mode": _first(q, "mode") or "",
    }

def normalize_ss_uri(uri: str) -> dict:
    # ss://method:password@host:port or SIP002 ss://base64(method:password@host:port)#tag
    u = urlparse(uri)
    netloc = u.netloc
    if "@" not in netloc:
        # base64-encoded part (may include tag after '#')
        b64 = netloc
        if "#" in b64:
            b64 = b64.split("#", 1)[0]
        padded = b64 + "=" * (-len(b64) % 4)
        decoded = base64.urlsafe_b64decode(padded.encode("utf-8")).decode("utf-8")
        # decoded form: method:password@host:port
        netloc = decoded

    m = re.match(r"([^:]+):([^@]+)@([^:]+):(\d+)", netloc)
    method = password = host = port = ""
    if m:
        method, password, host, port = m.groups()

    q = parse_qs(u.query)
    return {
        "protocol": "shadowsocks",
        "method": method,
        "password": password,
        "host": host,
        "port": str(port),
        "plugin": _first(q, "plugin"),
        "plugin_opts": _first(q, "plugin-opts") or _first(q, "plugin_opts") or "",
    }

def normalize_xray_json(root: dict) -> dict:
    """
    نرمال‌سازی JSON کامل Xray/V2Ray:
    - protocol
    - تنظیمات کلاینت/سرور مهم
    - streamSettings (tls/reality/ws/grpc/tcp/kcp/quic/http)
    """
    out = {}
    protocol = root.get("protocol") or ""
    out["protocol"] = protocol

    settings = root.get("settings") or {}
    stream = root.get("streamSettings") or {}

    # Common top-level: tag حذف می‌شود طبق منطق قبلی
    # برای هر پروتکل، کلاینت/سرورهای اصلی را برداشت می‌کنیم
    if protocol in ("vmess", "vless"):
        # vnext array → first entry canonical
        vnext = settings.get("vnext") or []
        if vnext:
            v0 = vnext[0]
            out["address"] = v0.get("address")
            out["port"] = str(v0.get("port") or "")
            clients = v0.get("users") or []
            if clients:
                c0 = clients[0]
                out["id"] = c0.get("id")
                out["alterId"] = str(c0.get("alterId") or c0.get("aid") or "")
                out["security"] = c0.get("security") or c0.get("encryption") or ""
                out["flow"] = c0.get("flow") or ""
    elif protocol == "trojan":
        servers = settings.get("servers") or []
        if servers:
            s0 = servers[0]
            out["address"] = s0.get("address")
            out["port"] = str(s0.get("port") or "")
            out["password"] = _first_scalar(s0.get("password"), s0.get("passwords"))
    elif protocol == "shadowsocks":
        servers = settings.get("servers") or []
        if servers:
            s0 = servers[0]
            out["address"] = s0.get("address")
            out["port"] = str(s0.get("port") or "")
            out["method"] = s0.get("method")
            out["password"] = s0.get("password")
            out["plugin"] = s0.get("plugin") or ""
            out["plugin_opts"] = s0.get("pluginOpts") or s0.get("plugin-opts") or ""
    # دیگر پروتکل‌ها را می‌توان مشابه افزود

    # streamSettings
    if stream:
        out["network"] = stream.get("network") or ""
        out["security"] = stream.get("security") or ""
        out["tlsServerName"] = stream.get("serverName") or stream.get("tlsServerName") or ""
        out["alpn"] = _to_list_str(stream.get("alpn"))
        # TLS
        if "tlsSettings" in stream:
            tls = stream.get("tlsSettings") or {}
            out["tls_sni"] = tls.get("serverName") or ""
            out["tls_alpn"] = _to_list_str(tls.get("alpn"))
            out["tls_fp"] = tls.get("fingerprint") or ""
        # REALITY
        if "realitySettings" in stream:
            rs = stream.get("realitySettings") or {}
            out["reality_serverNames"] = _to_list_str(rs.get("serverNames"))
            out["reality_privateKey"] = rs.get("privateKey") or ""
            out["reality_shortIds"] = _to_list_str(rs.get("shortIds"))
            out["reality_publicKey"] = rs.get("publicKey") or ""
        # WS
        if "wsSettings" in stream:
            ws = stream.get("wsSettings") or {}
            out["ws_path"] = ws.get("path") or ""
            out["ws_host"] = _first_header_host(ws.get("headers"))
        # gRPC
        if "grpcSettings" in stream:
            g = stream.get("grpcSettings") or {}
            out["grpc_serviceName"] = g.get("serviceName") or ""
            out["grpc_mode"] = g.get("multiMode") or g.get("mode") or ""
        # HTTP/2 (h2)
        if "httpSettings" in stream:
            h = stream.get("httpSettings") or {}
            out["http_path"] = _to_list_str(h.get("path"))
            out["http_host"] = _to_list_str(h.get("host"))
        # TCP header
        if "tcpSettings" in stream:
            t = stream.get("tcpSettings") or {}
            h = t.get("header") or {}
            out["tcp_header_type"] = h.get("type") or ""
        # KCP
        if "kcpSettings" in stream:
            k = stream.get("kcpSettings") or {}
            out["kcp_seed"] = k.get("seed") or ""
            h = k.get("header") or {}
            out["kcp_header_type"] = h.get("type") or ""
        # QUIC
        if "quicSettings" in stream:
            q = stream.get("quicSettings") or {}
            out["quic_security"] = q.get("security") or ""
            out["quic_key"] = q.get("key") or ""
            h = q.get("header") or {}
            out["quic_header_type"] = h.get("type") or ""

    return out

# ------------------ Helpers ------------------ #

def flatten_to_text(obj, prefix=""):
    """
    فلت‌کردن دترمینیستیک: دیکشنری با کلیدهای مرتب؛ لیست با ایندکس؛
    خروجی: خطوط "path=value" سپس join با '\n'.
    """
    items = []
    if isinstance(obj, dict):
        for k in sorted(obj.keys()):
            v = obj[k]
            new_prefix = f"{prefix}.{k}" if prefix else k
            items.extend(flatten_to_text(v, new_prefix))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            new_prefix = f"{prefix}[{i}]"
            items.extend(flatten_to_text(v, new_prefix))
    else:
        items.append(f"{prefix}={_scalar_to_str(obj)}")
    return items

def sha256_hex(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def _first(qs, key):
    return (qs.get(key) or [""])[0]

def _first_list(qs, key):
    return qs.get(key) or []

def _scalar_to_str(v):
    if v is None:
        return ""
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)

def _to_list_str(v):
    if v is None:
        return ""
    if isinstance(v, (list, tuple)):
        return ",".join(str(x) for x in v)
    return str(v)

def _first_scalar(*args):
    for v in args:
        if v:
            if isinstance(v, list) and v:
                return v[0]
            return v
    return ""

def _first_header_host(headers):
    if not headers:
        return ""
    if isinstance(headers, dict):
        # header keys are case-insensitive; host or Host
        return headers.get("host") or headers.get("Host") or ""
    return ""

# ------------------ CLI test (optional) ------------------ #
if __name__ == "__main__":
    import sys
    for line in sys.stdin:
        h = canonical_hash(line.strip())
        print(h)