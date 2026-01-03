import json
import hashlib
import sqlite3
import threading
from typing import Any, Dict, List, Optional, Tuple

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

    پروتکل‌های پشتیبانی‌شده:
      - vmess
      - vless
      - trojan
      - shadowsocks / shadowsocks2022 / ss

    نکته مهم (Fix برای SS):
      بعضی SSها ممکن است password خالی داشته باشند (""), که قبلاً باعث می‌شد hash تولید نشود.
      این نسخه حتی با password خالی هم برای SS هش پایدار تولید می‌کند.
    """

    def __init__(self, batch_size: int = 1000) -> None:
        self.batch_size = batch_size
        self.stats = {
            "total_candidates": 0,
            "batches": 0,
            "rows_processed": 0,
            "rows_hashed": 0,
            "rows_skipped_unsupported": 0,
            # legacy/compat
            "rows_skipped_non_vmess": 0,
            "json_decode_errors": 0,
            "identity_errors": 0,
            "marked_invalid": 0,
            # debug-ish
            "ss_hashed_with_empty_password": 0,
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
        # Canonical: کلیدها مرتب، بدون فاصله اضافه
        return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)

    @staticmethod
    def _sha256_hex(text: str) -> str:
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    @staticmethod
    def _safe_int(value: Any) -> Optional[int]:
        try:
            return int(str(value).strip())
        except Exception:
            return None

    @staticmethod
    def _safe_str(value: Any) -> Optional[str]:
        if value is None:
            return None
        s = str(value)
        s = s.strip()
        return s if s else None

    @staticmethod
    def _safe_str_allow_empty(value: Any) -> Optional[str]:
        """
        مثل _safe_str، ولی اگر رشته خالی بود "" را نگه می‌دارد (برای SS password).
        """
        if value is None:
            return None
        s = str(value)
        # whitespace های دو طرف را حذف می‌کنیم، ولی خالی شدن را مجاز می‌دانیم
        s = s.strip()
        return s  # ممکن است "" باشد

    @staticmethod
    def _norm_host(value: Any) -> Optional[str]:
        s = ConfigHashUpdater._safe_str(value)
        return s.lower() if s else None

    def _norm_cipher(self, value: Any) -> Optional[str]:
        """
        برای cipher/method:
          - اگر کاملاً ascii-ساده بود → lower()
          - اگر کاراکترهای عجیب داشت → همان را نگه می‌داریم (برای اینکه «روش» عوض نشود)
        """
        s = self._safe_str(value)
        if s is None:
            return None
        try:
            simple = all((("a" <= ch <= "z") or ("A" <= ch <= "Z") or ("0" <= ch <= "9") or ch in "-._+") for ch in s)
            return s.lower() if simple else s
        except Exception:
            return s

    # ---------- استخراج helperهای مشترک ----------

    def _extract_stream_fingerprint(self, stream: Dict[str, Any]) -> Dict[str, Any]:
        """
        بخش مشترک: network + tls/reality + sni + ws/http host/path
        خروجی: dict کوچک برای الحاق به identity
        """
        out: Dict[str, Any] = {}

        network = stream.get("network") or "tcp"
        security_layer = stream.get("security")  # tls / reality / none / ...
        tls_settings = stream.get("tlsSettings") or {}
        reality_settings = stream.get("realitySettings") or {}

        if not isinstance(tls_settings, dict):
            tls_settings = {}
        if not isinstance(reality_settings, dict):
            reality_settings = {}

        network_norm = self._norm_host(network) or "tcp"
        out["network"] = network_norm

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
                tls_type = tls_type or "tls"
            elif reality_settings:
                tls_enabled = True
                tls_type = tls_type or "reality"

        out["tls"] = bool(tls_enabled)
        if tls_type:
            out["tls_type"] = str(tls_type).strip().lower()

        sni = tls_server_name or reality_server_name
        sni_norm = self._norm_host(sni)
        if sni_norm:
            out["sni"] = sni_norm

        # ws/http host/path
        path = None
        host_header = None
        if network_norm in ("ws", "http", "h2", "h3"):
            ws = stream.get("wsSettings") or stream.get("httpSettings") or {}
            if isinstance(ws, dict):
                p = ws.get("path")
                if isinstance(p, str) and p.strip():
                    path = p.strip()
                headers = ws.get("headers") or {}
                if isinstance(headers, dict):
                    h = headers.get("Host") or headers.get("host")
                    if isinstance(h, str) and h.strip():
                        host_header = h.strip()

        host_norm = self._norm_host(host_header)
        if host_norm:
            out["host"] = host_norm
        if isinstance(path, str) and path.strip():
            out["path"] = path.strip()

        return out

    def _get_stream(self, outbound: Dict[str, Any]) -> Dict[str, Any]:
        stream = outbound.get("streamSettings") or {}
        return stream if isinstance(stream, dict) else {}

    # ---------- استخراج هویت پروتکل‌ها ----------

    def _extract_vmess_identity(self, outbound: Dict[str, Any]) -> Optional[Dict[str, Any]]:
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

            security = user.get("security")
            alter_id = user.get("alterId") or user.get("alter_id")

            address_norm = self._norm_host(address)
            port_int = self._safe_int(port)
            user_id_norm = self._norm_host(user_id)
            if not address_norm or port_int is None or not user_id_norm:
                return None

            ident: Dict[str, Any] = {
                "protocol": "vmess",
                "address": address_norm,
                "port": port_int,
                "user_id": user_id_norm,
            }

            sec_norm = self._norm_host(security)
            if sec_norm:
                ident["security"] = sec_norm

            if alter_id is not None:
                try:
                    ident["alter_id"] = int(str(alter_id).strip())
                except Exception:
                    ident["alter_id"] = str(alter_id).strip()

            stream_fp = self._extract_stream_fingerprint(self._get_stream(outbound))
            ident.update(stream_fp)

            return ident
        except Exception as e:
            print(f"[hash_updater] ERROR in _extract_vmess_identity: {e}")
            return None

    def _extract_vless_identity(self, outbound: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            if not isinstance(outbound, dict):
                return None

            settings = outbound.get("settings") or {}
            if not isinstance(settings, dict):
                return None

            address = None
            port = None
            user_id = None
            encryption = None
            flow = None

            vnext = settings.get("vnext")
            if isinstance(vnext, list) and vnext:
                server = vnext[0]
                if isinstance(server, dict):
                    address = server.get("address")
                    port = server.get("port")
                    users = server.get("users")
                    if isinstance(users, list) and users:
                        user = users[0]
                        if isinstance(user, dict):
                            user_id = user.get("id")
                            encryption = user.get("encryption")
                            flow = user.get("flow")

            if not address or not port or not user_id:
                address = settings.get("address") or address
                port = settings.get("port") or port
                user_id = settings.get("id") or settings.get("uuid") or user_id
                if encryption is None:
                    encryption = settings.get("encryption")
                if flow is None:
                    flow = settings.get("flow")

            address_norm = self._norm_host(address)
            port_int = self._safe_int(port)
            user_id_norm = self._norm_host(user_id)
            if not address_norm or port_int is None or not user_id_norm:
                return None

            ident: Dict[str, Any] = {
                "protocol": "vless",
                "address": address_norm,
                "port": port_int,
                "user_id": user_id_norm,
            }

            enc_norm = self._norm_host(encryption)
            if enc_norm:
                ident["encryption"] = enc_norm

            flow_norm = self._norm_host(flow)
            if flow_norm:
                ident["flow"] = flow_norm

            stream_fp = self._extract_stream_fingerprint(self._get_stream(outbound))
            ident.update(stream_fp)

            return ident
        except Exception as e:
            print(f"[hash_updater] ERROR in _extract_vless_identity: {e}")
            return None

    def _extract_trojan_identity(self, outbound: Dict[str, Any]) -> Optional[Dict[str, Any]]:
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

            address_norm = self._norm_host(address)
            port_int = self._safe_int(port)
            password_norm = self._safe_str(password)  # ممکن است حساس به case باشد
            if not address_norm or port_int is None or not password_norm:
                return None

            ident: Dict[str, Any] = {
                "protocol": "trojan",
                "address": address_norm,
                "port": port_int,
                "password": password_norm,
            }

            stream_fp = self._extract_stream_fingerprint(self._get_stream(outbound))
            ident.update(stream_fp)

            return ident
        except Exception as e:
            print(f"[hash_updater] ERROR in _extract_trojan_identity: {e}")
            return None

    def _extract_shadowsocks_identity(self, outbound: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Fix اصلی: password خالی ("") را هم قبول می‌کنیم تا SS hash تولید کند.
        """
        try:
            if not isinstance(outbound, dict):
                return None

            settings = outbound.get("settings") or {}
            if not isinstance(settings, dict):
                settings = {}

            address = None
            port = None
            method = None
            password = None
            uot = None
            plugin = None
            plugin_opts = None

            def pick_from_server_dict(srv: Dict[str, Any]) -> None:
                nonlocal address, port, method, password, uot, plugin, plugin_opts

                if address is None:
                    address = srv.get("address") or srv.get("server") or srv.get("addr")
                if port is None:
                    port = srv.get("port") or srv.get("server_port")

                # cipher/method
                if method is None:
                    method = srv.get("method") or srv.get("cipher")

                # password: اجازهٔ خالی بودن
                if password is None:
                    if "password" in srv:
                        password = srv.get("password")
                    elif "pass" in srv:
                        password = srv.get("pass")
                    elif "passwd" in srv:
                        password = srv.get("passwd")

                # SIP008 / بعضی مدل‌ها:
                users = srv.get("users")
                if isinstance(users, list) and users:
                    u0 = users[0]
                    if isinstance(u0, dict):
                        if method is None:
                            method = u0.get("method") or u0.get("cipher") or method
                        if password is None:
                            if "password" in u0:
                                password = u0.get("password")
                            elif "pass" in u0:
                                password = u0.get("pass")
                            elif "passwd" in u0:
                                password = u0.get("passwd")

                if uot is None and "uot" in srv:
                    uot = srv.get("uot")

                if plugin is None and "plugin" in srv:
                    plugin = srv.get("plugin")
                if plugin_opts is None and ("pluginOpts" in srv or "plugin_opts" in srv or "plugin-opts" in srv):
                    plugin_opts = srv.get("pluginOpts") or srv.get("plugin_opts") or srv.get("plugin-opts")

            servers = settings.get("servers")
            if isinstance(servers, list) and servers:
                s0 = servers[0]
                if isinstance(s0, dict):
                    pick_from_server_dict(s0)

            # fallback: مستقیم از settings
            if address is None:
                address = settings.get("address") or settings.get("server") or settings.get("addr")
            if port is None:
                port = settings.get("port") or settings.get("server_port")
            if method is None:
                method = settings.get("method") or settings.get("cipher")

            if password is None:
                # اگر اصلاً چیزی پیدا نشد، به جای skip کردن، خالی در نظر می‌گیریم
                # تا حداقل hash تولید شود (پایدار).
                password = ""

            if uot is None and "uot" in settings:
                uot = settings.get("uot")

            if plugin is None and "plugin" in settings:
                plugin = settings.get("plugin")
            if plugin_opts is None and ("pluginOpts" in settings or "plugin_opts" in settings or "plugin-opts" in settings):
                plugin_opts = settings.get("pluginOpts") or settings.get("plugin_opts") or settings.get("plugin-opts")

            address_norm = self._norm_host(address)
            port_int = self._safe_int(port)
            method_norm = self._norm_cipher(method)
            password_norm = self._safe_str_allow_empty(password)

            # address/port/method باید باشند؛ password می‌تواند خالی باشد.
            if not address_norm or port_int is None or not method_norm or password_norm is None:
                return None

            ident: Dict[str, Any] = {
                "protocol": "shadowsocks",
                "address": address_norm,
                "port": port_int,
                "method": method_norm,
                "password": password_norm,  # ممکن است ""
            }

            if password_norm == "":
                with self._stats_lock:
                    self.stats["ss_hashed_with_empty_password"] += 1

            if isinstance(uot, bool):
                ident["uot"] = uot

            plug = self._safe_str(plugin)
            if plug:
                ident["plugin"] = plug

            if plugin_opts is not None:
                # هرچی هست را canonical می‌کنیم تا ثابت بماند
                try:
                    if isinstance(plugin_opts, (dict, list)):
                        ident["plugin_opts"] = json.loads(json.dumps(plugin_opts))
                    else:
                        ident["plugin_opts"] = str(plugin_opts)
                except Exception:
                    ident["plugin_opts"] = str(plugin_opts)

            stream_fp = self._extract_stream_fingerprint(self._get_stream(outbound))
            ident.update(stream_fp)

            return ident
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

                updates: List[Tuple[str, int]] = []
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
                        invalid_ids.append(row_id)
                        with self._stats_lock:
                            self.stats["json_decode_errors"] += 1
                            self.stats["marked_invalid"] += 1
                        continue

                    try:
                        data = json.loads(raw_str)
                    except json.JSONDecodeError:
                        print(f"[hash_updater] JSON decode error for id={row_id}")
                        invalid_ids.append(row_id)
                        with self._stats_lock:
                            self.stats["json_decode_errors"] += 1
                            self.stats["marked_invalid"] += 1
                        continue

                    outbound: Optional[Dict[str, Any]] = None
                    proto: Optional[str] = None

                    if isinstance(data, dict):
                        p = data.get("protocol") or data.get("type")
                        if isinstance(p, str):
                            proto = p
                            outbound = data

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
                            self.stats["rows_skipped_non_vmess"] += 1
                        continue

                    if not ident:
                        with self._stats_lock:
                            self.stats["identity_errors"] += 1
                        continue

                    canonical = self._canonical_json_str(ident)
                    h = self._sha256_hex(canonical)
                    updates.append((h, row_id))

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
            f"\n ss hashed with empty password: {self.stats['ss_hashed_with_empty_password']}"
        )


if __name__ == "__main__":
    updater = ConfigHashUpdater(batch_size=1000)
    updater.update_hashes()
