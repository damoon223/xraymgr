import base64
import json
import re
import sqlite3
import threading
import urllib.parse
from typing import Any, Callable, Dict, List, Optional, Tuple

from . import jsbridge
from .schema import init_db_schema
from .settings import get_db_path


class JsonRepairStopped(Exception):
    """Signal برای توقف graceful JSON repair job."""
    pass


class JsonRepairUpdater:
    """
    هدف:
      - فقط روی ردیف‌های INVALID کار می‌کند (is_invalid=1 و is_protocol_unsupported=0)
      - url را دست نمی‌زند
      - اگر repair موفق شد: repaired_url را پر می‌کند (فقط وقتی JSON موفق استخراج نشود)
      - همان لحظه repaired_url را به Node می‌فرستد:
          - اگر JSON داد: config_json نوشته می‌شود + is_invalid=0 + repaired_url پاک می‌شود
          - اگر نداد: is_invalid همچنان 1 می‌ماند و repaired_url ذخیره می‌شود

    قوانین ویژه:
      - اگر پروتکل غیر از vmess/vless/ss/trojan باشد:
          is_invalid=0 و is_protocol_unsupported=1 می‌شود (بدون تلاش برای repair/convert) و repaired_url پاک می‌شود.

    نکته:
      - repaired_url شرط اسکیپ نیست (چون می‌خواهیم با تغییر منطق repair، دوباره خروجی بهتر تولید شود)
      - اگر repaired_url پر باشد ولی is_invalid=0 باشد، در شروع job پاک می‌شود.
    """

    SUPPORTED_PROTOCOLS = {"vmess", "vless", "trojan", "ss"}

    def __init__(self, batch_size: int = 1000, node_timeout: float = jsbridge.DEFAULT_NODE_TIMEOUT) -> None:
        self.batch_size = int(batch_size)
        self.node_timeout = float(node_timeout)

        self.stats: Dict[str, int] = {
            "total_candidates": 0,
            "batches": 0,
            "urls_seen": 0,
            "urls_repaired": 0,
            "urls_converted": 0,
            "urls_failed": 0,
            "rows_updated": 0,
            "rows_marked_valid": 0,
            "rows_repaired_cleared": 0,     # تعداد repaired_urlهایی که پاک شده‌اند
            "rows_marked_unsupported": 0,   # تعداد ردیف‌هایی که unsupported شدند
        }

        self._stop_event = threading.Event()
        self._stats_lock = threading.Lock()

        # ترتیب مهم است: هر روش جداگانه بررسی می‌کند؛ اگر applicable نبود None می‌دهد.
        self._repair_methods: List[Callable[[str, int], Optional[str]]] = [
            self._repair_vmess_payload_min_trim_to_valid_json,  # vmess: tail-trim char-by-char تا اولین JSON معتبر
            self._repair_ss_userinfo_b64_reencode,              # ss: اگر '@' داشت، encode صحیح / یا JSON->b64
            self._repair_ss_full_b64_to_reencoded,              # ss: legacy/full-b64 یا JSON-ish -> encode صحیح
            self._repair_strip_fragment_tag,                    # عمومی: حذف fragment (#tag)
            self._repair_strip_controls_and_whitespace,          # عمومی: پاکسازی کنترل‌کاراکترها/فاصله‌ها
        ]

    # ---------- stop control ----------

    def request_stop(self) -> None:
        print("[json_repair] stop requested")
        self._stop_event.set()

    def _check_stopped(self) -> None:
        if self._stop_event.is_set():
            raise JsonRepairStopped()

    # ---------- backward-compat API ----------

    def repair_invalid_then_run_json_updater(self) -> None:
        self.repair_and_fill_json()

    # ---------- helpers ----------

    @staticmethod
    def _detect_protocol(url: str) -> Optional[str]:
        if not url:
            return None
        s = str(url).strip()
        m = re.match(r"^([a-zA-Z0-9+.\-]+)://", s)
        if not m:
            return None
        p = (m.group(1) or "").strip().lower()
        return p or None

    @staticmethod
    def _canonical_json(obj: Any) -> str:
        if isinstance(obj, str):
            return obj.strip()
        if isinstance(obj, (dict, list)):
            return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)

    @staticmethod
    def _add_b64_padding(s: str) -> str:
        s = s.strip()
        pad = (-len(s)) % 4
        if pad:
            s += "=" * pad
        return s

    @staticmethod
    def _safe_text(b: bytes) -> str:
        try:
            return b.decode("utf-8", errors="strict")
        except Exception:
            return b.decode("utf-8", errors="ignore")

    @classmethod
    def _b64_try_decode_once(cls, s: str) -> Optional[bytes]:
        """
        یک تلاش decode بدون trim:
          - whitespaceهای داخل payload حذف می‌شوند
          - padding به‌صورت safe اضافه می‌شود
          - هم standard و هم urlsafe امتحان می‌شود
        """
        if not s:
            return None
        s = re.sub(r"\s+", "", s.strip())
        if not s:
            return None

        # standard
        try:
            return base64.b64decode(cls._add_b64_padding(s), validate=False)
        except Exception:
            pass

        # urlsafe
        try:
            return base64.urlsafe_b64decode(cls._add_b64_padding(s))
        except Exception:
            return None

    def _b64_decode_text_min_trim(self, payload: str, max_trim: int = 64) -> Optional[str]:
        """
        - از انتهای payload کاراکتر به کاراکتر کم می‌کند
        - اولین جایی که decode معتبر شد همان‌جا می‌ایستد (کمترین trim ممکن)
        """
        if not payload:
            return None

        payload = re.sub(r"\s+", "", str(payload).strip())
        if not payload:
            return None

        for k in range(0, max_trim + 1):
            if k == 0:
                cand = payload
            else:
                if len(payload) <= k:
                    break
                cand = payload[:-k]

            decoded = self._b64_try_decode_once(cand)
            if decoded is None:
                continue

            txt = self._safe_text(decoded).strip()
            if txt:
                return txt

        return None

    @staticmethod
    def _extract_minified_json_from_text(text: str) -> Optional[str]:
        """
        اگر داخل متن یک JSON object/array معتبر باشد، همان را (minified) برمی‌گرداند.
        """
        if not text:
            return None
        s = str(text)
        i_obj = s.find("{")
        i_arr = s.find("[")
        if i_obj < 0 and i_arr < 0:
            return None
        start = i_obj if (i_obj >= 0 and (i_arr < 0 or i_obj < i_arr)) else i_arr

        try:
            obj, _ = json.JSONDecoder().raw_decode(s[start:])
        except Exception:
            return None

        if not isinstance(obj, (dict, list)):
            return None

        return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))

    @staticmethod
    def _b64_urlsafe_no_padding(text: str) -> str:
        b = text.encode("utf-8", errors="strict")
        return base64.urlsafe_b64encode(b).decode("ascii").rstrip("=")

    @staticmethod
    def _b64_standard_no_padding(text: str) -> str:
        b = text.encode("utf-8", errors="strict")
        return base64.b64encode(b).decode("ascii").rstrip("=")

    @staticmethod
    def _looks_like_hostport(rest: str) -> bool:
        """
        rest در ss://<userinfo>@<rest> باید حداقل host:port باشد.
        rest ممکن است بعدش /?plugin... یا ?plugin... هم داشته باشد.
        """
        if not rest:
            return False
        r = rest.strip()

        base = r
        for sep in ("/", "?"):
            if sep in base:
                base = base.split(sep, 1)[0]
        base = base.strip()
        if not base:
            return False

        # IPv6 در براکت
        if base.startswith("["):
            if "]" not in base:
                return False
            _, _, port_part = base.partition("]:")
            if not port_part:
                return False
            return port_part.isdigit()

        if ":" not in base:
            return False
        host, port = base.rsplit(":", 1)
        if not host or not port.isdigit():
            return False
        return True

    # ---------- repair methods (هرکدام جداگانه) ----------

    def _repair_strip_controls_and_whitespace(self, url: str, _row_id: int) -> Optional[str]:
        s = str(url or "")
        if not s:
            return None
        cleaned = "".join(ch for ch in s if ch >= " " and ch != "\x7f").strip()
        if cleaned and cleaned != s:
            return cleaned
        return None

    def _repair_strip_fragment_tag(self, url: str, _row_id: int) -> Optional[str]:
        """
        tag اصلاً مهم نیست → کل fragment را حذف می‌کنیم.
        """
        s = str(url or "").strip()
        if not s or "#" not in s:
            return None
        base = s.split("#", 1)[0].strip()
        if base and base != s:
            return base
        return None

    def _repair_vmess_payload_min_trim_to_valid_json(self, url: str, _row_id: int) -> Optional[str]:
        """
        vmess://<base64(json)> + garbage
        - payload را tail-trim char-by-char می‌کنیم تا decode+JSON معتبر شود
        - JSON را با JSONDecoder.raw_decode از اولین '{' استخراج می‌کنیم
        """
        s = str(url or "").strip()
        if not s.lower().startswith("vmess://"):
            return None

        payload = s[len("vmess://") :].strip()
        if not payload:
            return None

        if "#" in payload:
            payload = payload.split("#", 1)[0].strip()

        payload = re.sub(r"\s+", "", payload)
        if not payload:
            return None

        max_trim = 64
        for k in range(0, max_trim + 1):
            if k == 0:
                cand = payload
            else:
                if len(payload) <= k:
                    break
                cand = payload[:-k]

            decoded = self._b64_try_decode_once(cand)
            if decoded is None:
                continue

            text = self._safe_text(decoded)
            json_min = self._extract_minified_json_from_text(text)
            if not json_min:
                continue

            b64 = base64.b64encode(json_min.encode("utf-8")).decode("ascii").rstrip("=")
            repaired = f"vmess://{b64}"

            if repaired != s:
                return repaired
            return None

        return None

    def _repair_ss_userinfo_b64_reencode(self, url: str, _row_id: int) -> Optional[str]:
        """
        ss://<userinfo>@<rest>
          - اگر userinfo base64 است: decode با tail-trim
          - اگر decode شد و JSON بود:
              خروجی = ss://BASE64(JSON_minified)   (حتی اگر '@...' داشت، حذف می‌شود)
          - اگر decode شد و نتیجه method:password بود:
              اگر rest شبیه host:port بود → خروجی sip002: ss://b64(method:password)@host:port...
              اگر rest host:port نبود → چیزی نمی‌سازیم (None)
        """
        s = str(url or "").strip()
        if not s.lower().startswith("ss://"):
            return None

        body = s[len("ss://") :].strip()
        if not body:
            return None

        if "#" in body:
            body = body.split("#", 1)[0].strip()
        if not body or "@" not in body:
            return None

        userinfo, rest = body.split("@", 1)
        userinfo = urllib.parse.unquote(userinfo.strip())
        rest = rest.strip()
        if not userinfo:
            return None

        # اگر userinfo خودش plain باشد، این روش مسئولش نیست
        if ":" in userinfo:
            return None

        decoded = self._b64_decode_text_min_trim(userinfo, max_trim=64)
        if not decoded:
            return None
        decoded = decoded.strip()
        if "\n" in decoded:
            decoded = decoded.splitlines()[0].strip()

        # JSON-like داخل ss://... : خروجی باید دوباره base64 شود، بدون '@...'
        json_min = self._extract_minified_json_from_text(decoded)
        if json_min:
            enc = self._b64_urlsafe_no_padding(json_min)
            repaired = f"ss://{enc}"
            if repaired != s:
                return repaired
            return None

        # حالت استاندارد: method:password
        if ":" not in decoded:
            return None

        # باید host:port واقعی باشد
        if not self._looks_like_hostport(rest):
            return None

        enc_userinfo = self._b64_urlsafe_no_padding(decoded)
        repaired = f"ss://{enc_userinfo}@{rest}"
        if repaired != s:
            return repaired
        return None

    def _repair_ss_full_b64_to_reencoded(self, url: str, _row_id: int) -> Optional[str]:
        """
        SS legacy/full-b64:
          ss://BASE64(...) [/?plugin...][#tag]
        یا ss://BASE64(JSON) + garbage

        رفتار:
          - decode با tail-trim
          - اگر JSON بود → ss://BASE64(JSON_minified) (بدون tail/tag و بدون @...)
          - اگر method:pass@host:port بود:
              - اگر plugin tail داشت → sip002 (userinfo b64) + tail
              - اگر tail نداشت → ss://BASE64(plain_full) (کل plain را دوباره base64 می‌کنیم)
        """
        s = str(url or "").strip()
        if not s.lower().startswith("ss://"):
            return None

        body = s[len("ss://") :].strip()
        if not body:
            return None

        if "#" in body:
            body = body.split("#", 1)[0].strip()
        if not body:
            return None

        # اگر '@' دارد، این روش مسئولش نیست
        if "@" in body:
            return None

        # جدا کردن tail مربوط به plugin
        base_payload = body
        tail = ""
        if "/?" in base_payload:
            base_payload, after = base_payload.split("/?", 1)
            tail = "/?" + after
        elif "?" in base_payload:
            base_payload, after = base_payload.split("?", 1)
            tail = "?" + after

        base_payload = urllib.parse.unquote(base_payload.strip())
        if not base_payload:
            return None

        decoded = self._b64_decode_text_min_trim(base_payload, max_trim=64)
        if not decoded:
            return None

        decoded = decoded.strip()
        if "\n" in decoded:
            decoded = decoded.splitlines()[0].strip()

        # JSON-like: باید دوباره base64 شود و تمام (هیچ @ یا tag نباید بماند)
        json_min = self._extract_minified_json_from_text(decoded)
        if json_min:
            enc = self._b64_urlsafe_no_padding(json_min)
            repaired = f"ss://{enc}"
            if repaired != s:
                return repaired
            return None

        # ممکن است decode خودش "ss://..." باشد
        if decoded.lower().startswith("ss://"):
            decoded = decoded[5:].strip()

        if "@" not in decoded:
            return None

        userinfo_plain, hostport = decoded.split("@", 1)
        userinfo_plain = userinfo_plain.strip()
        hostport = hostport.strip()
        if not userinfo_plain or ":" not in userinfo_plain or not hostport:
            return None

        if not self._looks_like_hostport(hostport):
            return None

        if tail:
            enc_userinfo = self._b64_urlsafe_no_padding(userinfo_plain)
            repaired = f"ss://{enc_userinfo}@{hostport}{tail}"
            if repaired != s:
                return repaired
            return None

        plain_full = f"{userinfo_plain}@{hostport}"
        enc_full = self._b64_standard_no_padding(plain_full)
        repaired = f"ss://{enc_full}"
        if repaired != s:
            return repaired
        return None

    # ---------- node conversion ----------

    def _convert_to_outbound_json_text(self, url: str) -> Optional[str]:
        try:
            outbound = jsbridge.convert_to_outbound(url, timeout=self.node_timeout)
        except Exception:
            return None

        txt = self._canonical_json(outbound)
        if not txt:
            return None
        return txt

    # ---------- main job ----------

    def run(self) -> None:
        self.repair_and_fill_json()

    def repair_and_fill_json(self) -> None:
        print(f"[json_repair] starting job (batch_size={self.batch_size})")

        init_db_schema()

        conn = sqlite3.connect(get_db_path())
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA temp_store = MEMORY;")

        try:
            # 0) پاکسازی: اگر repaired_url پر است ولی is_invalid=0 است، repaired_url باید خالی شود
            try:
                cur0 = conn.cursor()
                cur0.execute(
                    """
                    UPDATE links
                    SET repaired_url = NULL
                    WHERE is_invalid = 0
                      AND repaired_url IS NOT NULL
                      AND TRIM(repaired_url) <> ''
                    """
                )
                cleared = cur0.rowcount if cur0.rowcount is not None else 0
                conn.commit()
                if cleared:
                    with self._stats_lock:
                        self.stats["rows_repaired_cleared"] += int(cleared)
                    print(f"[json_repair] cleared repaired_url for {cleared} non-invalid rows")
            except Exception as e:
                conn.rollback()
                print(f"[json_repair] WARN failed initial repaired_url cleanup: {e}")

            last_id = 0

            while True:
                self._check_stopped()

                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT id, url
                    FROM links
                    WHERE is_invalid = 1
                      AND is_protocol_unsupported = 0
                      AND url IS NOT NULL
                      AND TRIM(url) <> ''
                      AND id > ?
                    ORDER BY id ASC
                    LIMIT ?
                    """,
                    (last_id, self.batch_size),
                )
                rows = cur.fetchall()
                if not rows:
                    print("[json_repair] no more candidates, exiting loop.")
                    break

                batch_len = len(rows)
                with self._stats_lock:
                    self.stats["batches"] += 1
                    self.stats["total_candidates"] += batch_len

                print(f"[json_repair] batch loaded: {batch_len} rows (last_id before batch={last_id})")

                repaired_updates: List[Tuple[str, int]] = []   # (repaired_url, id) فقط برای مواردی که JSON نگرفتند
                clear_repaired_ids: List[int] = []             # هر موردی که معتبر شد/unsupported شد → repaired_url پاک شود
                success_updates: List[Tuple[str, int]] = []    # (config_json, id)
                mark_valid_ids: List[int] = []                 # ids to set is_invalid=0
                mark_unsupported_ids: List[int] = []           # ids to set is_invalid=0 & is_protocol_unsupported=1

                for r in rows:
                    self._check_stopped()

                    row_id = int(r["id"])
                    last_id = row_id

                    url = str(r["url"] or "").strip()
                    if not url:
                        continue

                    with self._stats_lock:
                        self.stats["urls_seen"] += 1

                    proto = self._detect_protocol(url)
                    if proto and proto not in self.SUPPORTED_PROTOCOLS:
                        mark_unsupported_ids.append(row_id)
                        clear_repaired_ids.append(row_id)
                        with self._stats_lock:
                            self.stats["rows_marked_unsupported"] += 1
                        print(f"[json_repair] id={row_id} protocol={proto!r} -> mark unsupported and clear invalid")
                        continue

                    repaired: Optional[str] = None
                    used_method: Optional[str] = None

                    for method in self._repair_methods:
                        out = method(url, row_id)
                        if out and out.strip():
                            repaired = out.strip()
                            used_method = method.__name__
                            break

                    if not repaired:
                        continue

                    with self._stats_lock:
                        self.stats["urls_repaired"] += 1

                    print(f"[json_repair] id={row_id} repaired by {used_method}: {url!r} -> {repaired!r}")

                    json_text = self._convert_to_outbound_json_text(repaired)
                    if json_text:
                        success_updates.append((json_text, row_id))
                        mark_valid_ids.append(row_id)
                        clear_repaired_ids.append(row_id)
                        with self._stats_lock:
                            self.stats["urls_converted"] += 1
                    else:
                        repaired_updates.append((repaired, row_id))
                        with self._stats_lock:
                            self.stats["urls_failed"] += 1
                        print(f"[json_repair] WARN convert failed for id={row_id} (kept invalid=1)")

                rows_changed = 0
                cur = conn.cursor()

                try:
                    cur.execute("BEGIN")

                    # 1) ذخیره repaired_url فقط برای موارد ناموفق در تبدیل
                    if repaired_updates:
                        cur.executemany(
                            "UPDATE links SET repaired_url = ? WHERE id = ?",
                            repaired_updates,
                        )
                        rows_changed += cur.rowcount

                    # 2) نوشتن config_json برای موارد موفق
                    if success_updates:
                        cur.executemany(
                            "UPDATE links SET config_json = ? WHERE id = ?",
                            success_updates,
                        )
                        rows_changed += cur.rowcount

                    # 3) برداشتن invalid برای موارد موفق
                    if mark_valid_ids:
                        cur.executemany(
                            "UPDATE links SET is_invalid = 0 WHERE id = ?",
                            [(i,) for i in mark_valid_ids],
                        )
                        rows_changed += cur.rowcount
                        with self._stats_lock:
                            self.stats["rows_marked_valid"] += len(mark_valid_ids)

                    # 4) برداشتن invalid و زدن unsupported برای پروتکل‌های غیرمجاز
                    if mark_unsupported_ids:
                        cur.executemany(
                            "UPDATE links SET is_invalid = 0, is_protocol_unsupported = 1 WHERE id = ?",
                            [(i,) for i in mark_unsupported_ids],
                        )
                        rows_changed += cur.rowcount

                    # 5) پاک کردن repaired_url برای مواردی که معتبر شدند یا unsupported شدند
                    if clear_repaired_ids:
                        cur.executemany(
                            "UPDATE links SET repaired_url = NULL WHERE id = ?",
                            [(i,) for i in clear_repaired_ids],
                        )
                        rows_changed += cur.rowcount
                        with self._stats_lock:
                            self.stats["rows_repaired_cleared"] += len(clear_repaired_ids)

                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    print(f"[json_repair] ERROR committing batch updates: {e}")

                if rows_changed:
                    with self._stats_lock:
                        self.stats["rows_updated"] += rows_changed

        except JsonRepairStopped:
            print("[json_repair] stopped by request.")
        finally:
            try:
                conn.close()
                print("[json_repair] DB connection closed.")
            except Exception:
                pass
            try:
                jsbridge.close_global_converter()
            except Exception:
                pass

        print(
            "[json_repair] job finished:"
            f"\n total candidates: {self.stats['total_candidates']}"
            f"\n batches: {self.stats['batches']}"
            f"\n urls seen: {self.stats['urls_seen']}"
            f"\n urls repaired: {self.stats['urls_repaired']}"
            f"\n urls converted: {self.stats['urls_converted']}"
            f"\n urls failed: {self.stats['urls_failed']}"
            f"\n rows updated: {self.stats['rows_updated']}"
            f"\n rows marked valid: {self.stats['rows_marked_valid']}"
            f"\n repaired_url cleared: {self.stats['rows_repaired_cleared']}"
            f"\n rows marked unsupported: {self.stats['rows_marked_unsupported']}"
        )


if __name__ == "__main__":
    job = JsonRepairUpdater(batch_size=1000)
    job.repair_and_fill_json()
