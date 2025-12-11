# app/xraymgr/hash_updater.py

import json
import hashlib
import sqlite3
import threading
from typing import Any, Dict, List, Optional

from .settings import get_db_path


class HashUpdaterStopped(Exception):
    """Signal برای توقف graceful Hash updater."""
    pass


class ConfigHashUpdater:
    """
    پر کردن ستون config_hash در جدول links برای کانفیگ‌هایی که:

    - config_json NOT NULL و خالی نیست
    - config_hash IS NULL یا ''
    - is_invalid = 0

    از چند پروتکل متداول پشتیبانی می‌کند:
    - vmess
    - vless
    - trojan
    - shadowsocks / ss
    """

    def __init__(self, batch_size: int = 1000) -> None:
        self.batch_size = batch_size
        self.stats = {
            "total_candidates": 0,
            "batches": 0,
            "rows_processed": 0,
            "rows_hashed": 0,
            "rows_skipped_unsupported": 0,
            # برای سازگاری عقب‌رو، معادل قبلی:
            "rows_skipped_non_vmess": 0,
            "json_decode_errors": 0,
            "identity_errors": 0,
            "marked_invalid": 0,
        }
        self._stop_event = threading.Event()
        self._stats_lock = threading.Lock()

    # ---------- کنترل توقف ----------

    def request_stop(self) -> None:
        print("[hash_updater] stop requested")
        self._stop_event.set()

    def _check_stopped(self) -> None:
        if self._stop_event.is_set():
            raise HashUpdaterStopped()

    # ---------- ابزارهای کمکی ----------

    @staticmethod
    def _canonical_json_str(obj: Dict[str, Any]) -> str:
        """
        JSON کاننیکال: sort_keys=True و بدون فاصله اضافه.
        """
        return json.dumps(obj, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _sha256_hex(text: str) -> str:
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    @staticmethod
    def _safe_int(value: Any) -> Optional[int]:
        try:
            return int(str(value).strip())
        except Exception:
            return None

    # ---------- استخراج هویت پروتکل‌ها ----------

    def _extract_vmess_identity(self, outbound: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        از یک outbound VMess (همان شیء با فیلد protocol/settings/streamSettings)
        فیلدهای هویتی را استخراج می‌کند.
        اگر اطلاعات کلیدی ناقص باشد → None.
        """
        try:
            if not isinstance(outbound, dict):
                return None

            settings = outbound.get("settings") or {}
            if not isinstance(settings, dict):
                return None

            vnext = settings.get("vnext")
            if not isinstance(vnext, list) or not vnext:
                return None

            server = vnext[0]
            if not isinstance(server, dict):
                return None

            address = server.get("address")
            port = server.get("port")
            users = server.get("users")

            if not address or not port or not isinstance(users, list) or not users:
                return None

            user = users[0]
            if not isinstance(user, dict):
                return None

            user_id = user.get("id") or user.get("uuid")
            if not user_id:
                return None

            security = user.get("security")  # auto / aes-128-gcm / none / ...
            alter_id = user.get("alterId") or user.get("alter_id")

            # stream / transport / tls / ws / reality / ...
            stream = outbound.get("streamSettings") or {}
            if not isinstance(stream, dict):
                stream = {}

            network = stream.get("network") or "tcp"
            security_layer = stream.get("security")  # tls / reality / none / ...

            tls_settings = stream.get("tlsSettings") or {}
            reality_settings = stream.get("realitySettings") or {}
            if not isinstance(tls_settings, dict):
                tls_settings = {}
            if not isinstance(reality_settings, dict):
                reality_settings = {}

            tls_server_name = tls_settings.get("serverName") or tls_settings.get("sni")
            reality_server_name = reality_settings.get("serverName")

            # TLS / Reality detection
            tls_type = None
            tls_enabled = False

            if isinstance(security_layer, str):
                sec_lc = security_layer.strip().lower()
                if sec_lc and sec_lc not in ("none", "plaintext"):
                    tls_enabled = True
                    tls_type = sec_lc

            # اگر security_layer خالی است ولی tlsSettings / realitySettings هست، TLS را فعال فرض کن
            if not tls_enabled:
                if tls_settings:
                    tls_enabled = True
                    if not tls_type:
                        tls_type = "tls"
                elif reality_settings:
                    tls_enabled = True
                    if not tls_type:
                        tls_type = "reality"

            sni = tls_server_name or reality_server_name

            # WebSocket / HTTP path + headers
            path = None
            host_header = None
            if isinstance(network, str) and network.strip().lower() in ("ws", "http", "h2", "h3"):
                ws = stream.get("wsSettings") or stream.get("httpSettings") or {}
                if isinstance(ws, dict):
                    p = ws.get("path")
                    if isinstance(p, str) and p.strip():
                        path = p
                    headers = ws.get("headers") or {}
                    if isinstance(headers, dict):
                        # Host یا host
                        h = headers.get("Host") or headers.get("host")
                        if isinstance(h, str) and h.strip():
                            host_header = h

            # نرمال‌سازی مقادیر
            address_norm = str(address).strip().lower()
            port_int = self._safe_int(port)
            if port_int is None:
                return None

            user_id_norm = str(user_id).strip().lower()
            security_norm = (str(security).strip().lower()) if isinstance(security, str) else ""
            network_norm = (str(network).strip().lower()) if isinstance(network, str) else "tcp"

            identity: Dict[str, Any] = {
                "protocol": "vmess",
                "address": address_norm,
                "port": port_int,
                "user_id": user_id_norm,
                "network": network_norm,
                "tls": bool(tls_enabled),
            }

            if security_norm:
                identity["security"] = security_norm

            if alter_id is not None:
                try:
                    identity["alter_id"] = int(str(alter_id).strip())
                except Exception:
                    # اگر عددی نیست، به‌عنوان string ذخیره کن
                    identity["alter_id"] = str(alter_id).strip()

            if tls_type:
                identity["tls_type"] = str(tls_type).strip().lower()

            if isinstance(sni, str) and sni.strip():
                identity["sni"] = sni.strip().lower()

            if isinstance(host_header, str) and host_header.strip():
                identity["host"] = host_header.strip().lower()

            if isinstance(path, str) and path.strip():
                identity["path"] = path.strip()

            return identity

        except Exception as e:
            print(f"[hash_updater] ERROR in _extract_vmess_identity: {e}")
            return None

    def _extract_vless_identity(self, outbound: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        استخراج هویت برای VLESS:
        address/port/id + لایهٔ ترنسپورت (network/tls/sni/host/path/flow/encryption)
        """
        try:
            if not isinstance(outbound, dict):
                return None

            settings = outbound.get("settings") or {}
            if not isinstance(settings, dict):
                return None

            vnext = settings.get("vnext")
            if not isinstance(vnext, list) or not vnext:
                return None

            server = vnext[0]
            if not isinstance(server, dict):
                return None

            address = server.get("address")
            port = server.get("port")
            users = server.get("users")

            if not address or not port or not isinstance(users, list) or not users:
                return None

            user = users[0]
            if not isinstance(user, dict):
                return None

            user_id = user.get("id")
            if not user_id:
                return None

            encryption = user.get("encryption")
            flow = user.get("flow")

            # streamSettings
            stream = outbound.get("streamSettings") or {}
            if not isinstance(stream, dict):
                stream = {}

            network = stream.get("network") or "tcp"
            security_layer = stream.get("security")  # tls / reality / none / ...

            tls_settings = stream.get("tlsSettings") or {}
            reality_settings = stream.get("realitySettings") or {}
            if not isinstance(tls_settings, dict):
                tls_settings = {}
            if not isinstance(reality_settings, dict):
                reality_settings = {}

            tls_server_name = tls_settings.get("serverName") or tls_settings.get("sni")
            reality_server_name = reality_settings.get("serverName")

            # TLS / Reality detection
            tls_type = None
            tls_enabled = False

            if isinstance(security_layer, str):
                sec_lc = security_layer.strip().lower()
                if sec_lc and sec_lc not in ("none", "plaintext"):
                    tls_enabled = True
                    tls_type = sec_lc

            if not tls_enabled:
                if tls_settings:
                    tls_enabled = True
                    if not tls_type:
                        tls_type = "tls"
                elif reality_settings:
                    tls_enabled = True
                    if not tls_type:
                        tls_type = "reality"

            sni = tls_server_name or reality_server_name

            # WebSocket / HTTP path + headers
            path = None
            host_header = None
            if isinstance(network, str) and network.strip().lower() in ("ws", "http", "h2", "h3"):
                ws = stream.get("wsSettings") or stream.get("httpSettings") or {}
                if isinstance(ws, dict):
                    p = ws.get("path")
                    if isinstance(p, str) and p.strip():
                        path = p
                    headers = ws.get("headers") or {}
                    if isinstance(headers, dict):
                        h = headers.get("Host") or headers.get("host")
                        if isinstance(h, str) and h.strip():
                            host_header = h

            # نرمال‌سازی
            address_norm = str(address).strip().lower()
            port_int = self._safe_int(port)
            if port_int is None:
                return None

            user_id_norm = str(user_id).strip().lower()
            network_norm = (str(network).strip().lower()) if isinstance(network, str) else "tcp"

            identity: Dict[str, Any] = {
                "protocol": "vless",
                "address": address_norm,
                "port": port_int,
                "user_id": user_id_norm,
                "network": network_norm,
                "tls": bool(tls_enabled),
            }

            if isinstance(encryption, str) and encryption.strip():
                identity["encryption"] = encryption.strip().lower()

            if isinstance(flow, str) and flow.strip():
                identity["flow"] = flow.strip().lower()

            if tls_type:
                identity["tls_type"] = str(tls_type).strip().lower()

            if isinstance(sni, str) and sni.strip():
                identity["sni"] = sni.strip().lower()

            if isinstance(host_header, str) and host_header.strip():
                identity["host"] = host_header.strip().lower()

            if isinstance(path, str) and path.strip():
                identity["path"] = path.strip()

            return identity

        except Exception as e:
            print(f"[hash_updater] ERROR in _extract_vless_identity: {e}")
            return None

    def _extract_trojan_identity(self, outbound: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        استخراج هویت برای Trojan:
        address/port/password + لایهٔ ترنسپورت (network/tls/sni/host/path)
        """
        try:
            if not isinstance(outbound, dict):
                return None

            settings = outbound.get("settings") or {}
            if not isinstance(settings, dict):
                return None

            servers = settings.get("servers")
            if not isinstance(servers, list) or not servers:
                return None

            server = servers[0]
            if not isinstance(server, dict):
                return None

            address = server.get("address")
            port = server.get("port")
            password = server.get("password")

            if not address or not port or not password:
                return None

            # streamSettings
            stream = outbound.get("streamSettings") or {}
            if not isinstance(stream, dict):
                stream = {}

            network = stream.get("network") or "tcp"
            security_layer = stream.get("security")

            tls_settings = stream.get("tlsSettings") or {}
            reality_settings = stream.get("realitySettings") or {}
            if not isinstance(tls_settings, dict):
                tls_settings = {}
            if not isinstance(reality_settings, dict):
                reality_settings = {}

            tls_server_name = tls_settings.get("serverName") or tls_settings.get("sni")
            reality_server_name = reality_settings.get("serverName")

            tls_type = None
            tls_enabled = False

            if isinstance(security_layer, str):
                sec_lc = security_layer.strip().lower()
                if sec_lc and sec_lc not in ("none", "plaintext"):
                    tls_enabled = True
                    tls_type = sec_lc

            if not tls_enabled:
                if tls_settings:
                    tls_enabled = True
                    if not tls_type:
                        tls_type = "tls"
                elif reality_settings:
                    tls_enabled = True
                    if not tls_type:
                        tls_type = "reality"

            sni = tls_server_name or reality_server_name

            path = None
            host_header = None
            if isinstance(network, str) and network.strip().lower() in ("ws", "http", "h2", "h3"):
                ws = stream.get("wsSettings") or stream.get("httpSettings") or {}
                if isinstance(ws, dict):
                    p = ws.get("path")
                    if isinstance(p, str) and p.strip():
                        path = p
                    headers = ws.get("headers") or {}
                    if isinstance(headers, dict):
                        h = headers.get("Host") or headers.get("host")
                        if isinstance(h, str) and h.strip():
                            host_header = h

            address_norm = str(address).strip().lower()
            port_int = self._safe_int(port)
            if port_int is None:
                return None

            # password را به‌صورت literal نگه می‌داریم (ممکن است case-sensitive باشد)
            password_norm = str(password).strip()
            network_norm = (str(network).strip().lower()) if isinstance(network, str) else "tcp"

            identity: Dict[str, Any] = {
                "protocol": "trojan",
                "address": address_norm,
                "port": port_int,
                "password": password_norm,
                "network": network_norm,
                "tls": bool(tls_enabled),
            }

            if tls_type:
                identity["tls_type"] = str(tls_type).strip().lower()

            if isinstance(sni, str) and sni.strip():
                identity["sni"] = sni.strip().lower()

            if isinstance(host_header, str) and host_header.strip():
                identity["host"] = host_header.strip().lower()

            if isinstance(path, str) and path.strip():
                identity["path"] = path.strip()

            return identity

        except Exception as e:
            print(f"[hash_updater] ERROR in _extract_trojan_identity: {e}")
            return None

    def _extract_shadowsocks_identity(self, outbound: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        استخراج هویت برای Shadowsocks:
        address/port/method/password + لایهٔ ترنسپورت (network/tls/sni/host/path)
        """
        try:
            if not isinstance(outbound, dict):
                return None

            settings = outbound.get("settings") or {}
            if not isinstance(settings, dict):
                return None

            servers = settings.get("servers")
            if not isinstance(servers, list) or not servers:
                return None

            server = servers[0]
            if not isinstance(server, dict):
                return None

            address = server.get("address")
            port = server.get("port")
            method = server.get("method")
            password = server.get("password")

            if not address or not port or not method or not password:
                return None

            # streamSettings
            stream = outbound.get("streamSettings") or {}
            if not isinstance(stream, dict):
                stream = {}

            network = stream.get("network") or "tcp"
            security_layer = stream.get("security")

            tls_settings = stream.get("tlsSettings") or {}
            reality_settings = stream.get("realitySettings") or {}
            if not isinstance(tls_settings, dict):
                tls_settings = {}
            if not isinstance(reality_settings, dict):
                reality_settings = {}

            tls_server_name = tls_settings.get("serverName") or tls_settings.get("sni")
            reality_server_name = reality_settings.get("serverName")

            tls_type = None
            tls_enabled = False

            if isinstance(security_layer, str):
                sec_lc = security_layer.strip().lower()
                if sec_lc and sec_lc not in ("none", "plaintext"):
                    tls_enabled = True
                    tls_type = sec_lc

            if not tls_enabled:
                if tls_settings:
                    tls_enabled = True
                    if not tls_type:
                        tls_type = "tls"
                elif reality_settings:
                    tls_enabled = True
                    if not tls_type:
                        tls_type = "reality"

            sni = tls_server_name or reality_server_name

            path = None
            host_header = None
            if isinstance(network, str) and network.strip().lower() in ("ws", "http", "h2", "h3"):
                ws = stream.get("wsSettings") or stream.get("httpSettings") or {}
                if isinstance(ws, dict):
                    p = ws.get("path")
                    if isinstance(p, str) and p.strip():
                        path = p
                    headers = ws.get("headers") or {}
                    if isinstance(headers, dict):
                        h = headers.get("Host") or headers.get("host")
                        if isinstance(h, str) and h.strip():
                            host_header = h

            address_norm = str(address).strip().lower()
            port_int = self._safe_int(port)
            if port_int is None:
                return None

            method_norm = str(method).strip().lower()
            password_norm = str(password).strip()
            network_norm = (str(network).strip().lower()) if isinstance(network, str) else "tcp"

            identity: Dict[str, Any] = {
                "protocol": "shadowsocks",
                "address": address_norm,
                "port": port_int,
                "method": method_norm,
                "password": password_norm,
                "network": network_norm,
                "tls": bool(tls_enabled),
            }

            if tls_type:
                identity["tls_type"] = str(tls_type).strip().lower()

            if isinstance(sni, str) and sni.strip():
                identity["sni"] = sni.strip().lower()

            if isinstance(host_header, str) and host_header.strip():
                identity["host"] = host_header.strip().lower()

            if isinstance(path, str) and path.strip():
                identity["path"] = path.strip()

            return identity

        except Exception as e:
            print(f"[hash_updater] ERROR in _extract_shadowsocks_identity: {e}")
            return None

    # ---------- منطق اصلی ----------

    def update_hashes(self) -> None:
        print(f"[hash_updater] starting config_hash update job (batch_size={self.batch_size})")

        conn = sqlite3.connect(get_db_path())
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA temp_store = MEMORY;")

        try:
            last_id = 0

            while True:
                self._check_stopped()

                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT id, config_json
                    FROM links
                    WHERE config_json IS NOT NULL
                      AND TRIM(config_json) <> ''
                      AND (config_hash IS NULL OR config_hash = '')
                      AND is_invalid = 0
                      AND id > ?
                    ORDER BY id
                    LIMIT ?
                    """,
                    (last_id, self.batch_size),
                )
                rows = cur.fetchall()
                if not rows:
                    print("[hash_updater] no more candidates, exiting loop.")
                    break

                batch_count = len(rows)
                with self._stats_lock:
                    self.stats["batches"] += 1
                    self.stats["total_candidates"] += batch_count

                print(
                    f"[hash_updater] batch loaded: {batch_count} rows "
                    f"(last_id before batch={last_id})"
                )

                updates: List[tuple] = []
                invalid_ids: List[int] = []

                for row in rows:
                    self._check_stopped()

                    row_id = int(row["id"])
                    last_id = row_id
                    raw = row["config_json"]

                    with self._stats_lock:
                        self.stats["rows_processed"] += 1

                    if raw is None:
                        continue

                    raw_str = str(raw)
                    if not raw_str.strip():
                        # JSON خالی → invalid
                        invalid_ids.append(row_id)
                        with self._stats_lock:
                            self.stats["json_decode_errors"] += 1
                            self.stats["marked_invalid"] += 1
                        continue

                    # parse JSON
                    try:
                        data = json.loads(raw_str)
                    except json.JSONDecodeError:
                        print(f"[hash_updater] JSON decode error for id={row_id}")
                        invalid_ids.append(row_id)
                        with self._stats_lock:
                            self.stats["json_decode_errors"] += 1
                            self.stats["marked_invalid"] += 1
                        continue

                    # پیدا کردن outbound و protocol
                    outbound: Optional[Dict[str, Any]] = None
                    proto: Optional[str] = None

                    if isinstance(data, dict):
                        # حالت: خود outbound
                        p = data.get("protocol") or data.get("type")
                        if isinstance(p, str):
                            proto = p
                            outbound = data

                        # حالت: full config با outbounds
                        if proto is None:
                            outbounds = data.get("outbounds")
                            if isinstance(outbounds, list):
                                for ob in outbounds:
                                    if not isinstance(ob, dict):
                                        continue
                                    p2 = ob.get("protocol") or ob.get("type")
                                    if isinstance(p2, str):
                                        proto = p2
                                        outbound = ob
                                        break

                    if not proto or not isinstance(outbound, dict):
                        with self._stats_lock:
                            self.stats["identity_errors"] += 1
                        continue

                    proto_lc = proto.strip().lower()

                    # انتخاب extractor بر اساس پروتکل
                    if proto_lc == "vmess":
                        ident = self._extract_vmess_identity(outbound)
                    elif proto_lc == "vless":
                        ident = self._extract_vless_identity(outbound)
                    elif proto_lc == "trojan":
                        ident = self._extract_trojan_identity(outbound)
                    elif proto_lc in ("shadowsocks", "shadowsocks2022", "ss"):
                        ident = self._extract_shadowsocks_identity(outbound)
                    else:
                        with self._stats_lock:
                            self.stats["rows_skipped_unsupported"] += 1
                            # حفظ شمارندهٔ قدیمی برای ابزارهای بیرونی که شاید از آن استفاده کنند
                            self.stats["rows_skipped_non_vmess"] += 1
                        continue

                    if not ident:
                        with self._stats_lock:
                            self.stats["identity_errors"] += 1
                        continue

                    canonical = self._canonical_json_str(ident)
                    h = self._sha256_hex(canonical)
                    updates.append((h, row_id))

                # اعمال آپدیت‌ها
                cur = conn.cursor()

                if updates:
                    try:
                        cur.execute("BEGIN")
                        cur.executemany(
                            "UPDATE links SET config_hash = ? WHERE id = ?",
                            updates,
                        )
                        conn.commit()
                        rows_affected = cur.rowcount
                    except Exception as e:
                        conn.rollback()
                        print(f"[hash_updater] ERROR committing hash updates: {e}")
                        rows_affected = 0

                    with self._stats_lock:
                        self.stats["rows_hashed"] += len(updates)

                    print(
                        f"[hash_updater] batch hash updated: {len(updates)} rows in memory, "
                        f"sqlite rowcount={rows_affected}"
                    )

                if invalid_ids:
                    try:
                        cur.execute("BEGIN")
                        cur.executemany(
                            "UPDATE links SET is_invalid = 1 WHERE id = ?",
                            [(rid,) for rid in invalid_ids],
                        )
                        conn.commit()
                    except Exception as e:
                        conn.rollback()
                        print(f"[hash_updater] ERROR marking invalid rows: {e}")
                    else:
                        print(
                            f"[hash_updater] marked {len(invalid_ids)} rows as invalid "
                            f"(json_decode_errors or empty json)"
                        )

        except HashUpdaterStopped:
            print("[hash_updater] stopped by request.")
        finally:
            try:
                conn.close()
                print("[hash_updater] DB connection closed.")
            except Exception:
                pass

        print(
            "[hash_updater] job finished:"
            f"\n total candidate rows: {self.stats['total_candidates']}"
            f"\n batches: {self.stats['batches']}"
            f"\n rows processed: {self.stats['rows_processed']}"
            f"\n rows hashed: {self.stats['rows_hashed']}"
            f"\n rows skipped (unsupported proto):{self.stats['rows_skipped_unsupported']}"
            f"\n rows skipped (non-vmess legacy):{self.stats['rows_skipped_non_vmess']}"
            f"\n json decode errors: {self.stats['json_decode_errors']}"
            f"\n identity errors: {self.stats['identity_errors']}"
            f"\n marked invalid: {self.stats['marked_invalid']}"
        )


if __name__ == "__main__":
    updater = ConfigHashUpdater(batch_size=1000)
    updater.update_hashes()
