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
from .tag_updater import OutboundTagUpdater


class JsonRepairStopped(Exception):
    """Signal برای توقف graceful JSON repair job."""
    pass


class JsonRepairUpdater:
    """
    Repair و ساخت config_json برای لینک‌های invalid.

    ورودی:
      - ردیف‌هایی از links که is_invalid=1 و is_protocol_unsupported=0 هستند.

    خروجی:
      - اگر repair/convert موفق شد:
          config_json نوشته می‌شود
          is_invalid=0
          repaired_url پاک می‌شود
      - اگر موفق نشد:
          is_invalid=1 باقی می‌ماند
          repaired_url ذخیره می‌شود

    قوانین ویژه:
      - اگر پروتکل غیر از vmess/vless/ss/trojan باشد:
          is_invalid=0 و is_protocol_unsupported=1 می‌شود (بدون تلاش برای repair/convert) و repaired_url پاک می‌شود.

    نکته:
      - repaired_url شرط اسکیپ نیست (چون می‌خواهیم با تغییر منطق repair، دوباره خروجی بهتر تولید شود)
      - اگر repaired_url پر باشد ولی is_invalid=0 باشد، در شروع job پاک می‌شود.

    تغییر جدید:
      - config_json در نهایت tag را از روی ستون outbound_tag جایگزین/ست می‌کند.
      - اگر outbound_tag خالی باشد، برای همان id با UPDATE OR IGNORE تگ تولید می‌شود.
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
            "marked_unsupported": 0,
            "cleared_repaired_url": 0,
        }

        self._stop_event = threading.Event()
        self._stats_lock = threading.Lock()
        self._tag_helper = OutboundTagUpdater(batch_size=1)

    # ---------- stop ----------
    def request_stop(self) -> None:
        print("[json_repair] stop requested")
        self._stop_event.set()
        try:
            jsbridge.close_global_converter()
        except Exception:
            pass

    def _check_stopped(self) -> None:
        if self._stop_event.is_set():
            raise JsonRepairStopped()

    # ---------- helpers ----------
    @staticmethod
    def _detect_protocol(url: str) -> Optional[str]:
        s = (url or "").strip()
        if not s:
            return None
        m = re.match(r"^([a-zA-Z0-9\-\+]+)://", s)
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
    def _apply_outbound_tag(outbound: Any, outbound_tag: str) -> Any:
        tag = (outbound_tag or "").strip()
        if not tag:
            raise ValueError("empty outbound_tag")

        obj: Any = outbound
        if isinstance(obj, str):
            s = obj.strip()
            if not s:
                raise ValueError("empty JSON string from bridge")
            obj = json.loads(s)  # اگر JSON نبود exception می‌دهد و caller invalid می‌کند

        if isinstance(obj, dict):
            obj["tag"] = tag
            return obj

        if isinstance(obj, list) and len(obj) == 1 and isinstance(obj[0], dict):
            obj[0]["tag"] = tag
            return obj

        raise ValueError(f"unexpected outbound type for tag injection: {type(obj).__name__}")

    def _ensure_outbound_tag_for_id(self, conn: sqlite3.Connection, row_id: int, existing: Optional[str]) -> str:
        """
        اگر outbound_tag خالی باشد، با UPDATE OR IGNORE و UNIQUE INDEX آن را ست می‌کند.
        این مسیر باید rarely-hit باشد (پیش‌فرض: outbound_tag قبلاً با tag_updater پر شده است).
        """
        if existing and str(existing).strip():
            return str(existing).strip()

        for _ in range(20):
            tag = self._tag_helper.generate_tag()
            cur = conn.cursor()
            cur.execute(
                "UPDATE OR IGNORE links SET outbound_tag = ? "
                "WHERE id = ? AND (outbound_tag IS NULL OR TRIM(outbound_tag) = '')",
                (tag, row_id),
            )
            conn.commit()
            cur.execute("SELECT outbound_tag FROM links WHERE id = ?", (row_id,))
            r = cur.fetchone()
            if r and r[0] and str(r[0]).strip():
                return str(r[0]).strip()

        raise ValueError(f"could not allocate unique outbound_tag for id={row_id}")

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
        تلاش برای base64 decode یک‌بار. اگر شکست خورد None برمی‌گرداند.
        """
        s2 = cls._add_b64_padding(s)
        try:
            return base64.b64decode(s2, validate=False)
        except Exception:
            return None

    @staticmethod
    def _strip_fragment(url: str) -> str:
        # حذف #... (نام/remark) که گاهی باعث خراب شدن parse می‌شود
        if "#" in url:
            return url.split("#", 1)[0]
        return url

    @staticmethod
    def _strip_controls(s: str) -> str:
        # حذف کنترل‌کاراکترها
        return "".join(ch for ch in s if ch >= " " and ch != "\x7f").strip()

    # ---------- vmess repair ----------
    def _repair_vmess(self, url: str) -> Optional[str]:
        """
        vmess://<base64(json)>  با tail/trailing-garbage
        استراتژی:
          - payload را بگیر
          - base64 decode (با padding)
          - متن JSON را پیدا کن (آخرین })
          - هر چیزی بعد از آن را حذف کن
          - اگر لازم شد از tail کم کن تا json.loads موفق شود
          - سپس json را مجدد base64 کن و لینک استاندارد بساز
        """
        s = self._strip_fragment(url)
        if not s.lower().startswith("vmess://"):
            return None
        payload = s[len("vmess://") :].strip()
        payload = self._strip_controls(payload)

        b = self._b64_try_decode_once(payload)
        if not b:
            return None

        txt = self._safe_text(b)
        txt = self._strip_controls(txt)

        # تلاش مستقیم
        def try_parse(j: str) -> Optional[dict]:
            try:
                return json.loads(j)
            except Exception:
                return None

        obj = try_parse(txt)
        if obj is None:
            # بریدن بعد از آخرین }
            last = txt.rfind("}")
            if last != -1:
                txt2 = txt[: last + 1]
                obj = try_parse(txt2)
                if obj is None:
                    # fallback: حذف تدریجی tail
                    for k in range(1, min(200, len(txt2))):
                        obj = try_parse(txt2[:-k])
                        if obj is not None:
                            txt2 = txt2[:-k]
                            break
                if obj is None:
                    return None
                txt = txt2
            else:
                return None

        # canonicalize json then re-encode
        canonical = json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        enc = base64.b64encode(canonical.encode("utf-8")).decode("ascii")
        return "vmess://" + enc

    # ---------- ss repair ----------
    def _repair_ss(self, url: str) -> Optional[str]:
        """
        ss://... حالت‌های مختلف
        استراتژی:
          - fragment را حذف کن
          - اگر userinfo به شکل base64 است، padding/encode را normalize کن
        """
        s = self._strip_fragment(url)
        if not s.lower().startswith("ss://"):
            return None
        s = self._strip_controls(s)

        # برخی ssها به شکل ss://base64(method:pass)@host:port
        # یا ss://base64(method:pass@host:port)
        body = s[len("ss://") :]

        # اگر @ وجود دارد، بخش قبل @ را normalize می‌کنیم (userinfo)
        if "@" in body:
            left, right = body.split("@", 1)
            left = left.strip()
            if not left:
                return None
            # اگر left شامل : باشد ممکن است decoded باشد؛ ولی برای robust بودن تلاش decode می‌کنیم
            b = self._b64_try_decode_once(left)
            if b:
                userinfo = self._safe_text(b)
                userinfo = self._strip_controls(userinfo)
                # دوباره base64 استاندارد
                left2 = base64.b64encode(userinfo.encode("utf-8")).decode("ascii")
                return "ss://" + left2 + "@" + right.strip()
            return "ss://" + left + "@" + right.strip()

        # اگر @ ندارد، ممکن است کل payload base64 باشد
        b = self._b64_try_decode_once(body.strip())
        if not b:
            return None
        decoded = self._safe_text(b)
        decoded = self._strip_controls(decoded)

        # اگر decoded شامل @ باشد، یعنی روش دوم: method:pass@host:port
        if "@" in decoded:
            left2 = base64.b64encode(decoded.encode("utf-8")).decode("ascii")
            return "ss://" + left2

        return None

    # ---------- generic repair ----------
    def _repair_url(self, url: str) -> Tuple[Optional[str], Optional[str]]:
        """
        خروجی:
          (repaired_url, reason)
        """
        proto = self._detect_protocol(url)
        if not proto:
            return None, "no_protocol"

        proto = proto.lower()

        # حذف fragment عمومی
        clean = self._strip_fragment(url)

        if proto == "vmess":
            r = self._repair_vmess(clean)
            return r, "vmess_repair" if r else (None, "vmess_no_repair")[1]
        if proto == "ss":
            r = self._repair_ss(clean)
            return r, "ss_repair" if r else (None, "ss_no_repair")[1]

        # vless/trojan معمولاً repair خاصی ندارند؛ فقط fragment/controls را تمیز می‌کنیم
        if proto in {"vless", "trojan"}:
            return clean.strip(), "strip_fragment"

        return None, "unsupported_proto"

    # ---------- node conversion ----------
    def _convert_to_outbound_json_text(self, url: str, outbound_tag: str) -> Optional[str]:
        try:
            outbound = jsbridge.convert_to_outbound(url, timeout=self.node_timeout)
        except Exception:
            return None

        try:
            outbound = self._apply_outbound_tag(outbound, outbound_tag)
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

        # اطمینان از وجود ستون/ایندکس‌های تگ (برای ست کردن tag در config_json)
        try:
            self._tag_helper.ensure_tag_schema()
        except Exception as e:
            print(f"[json_repair] WARN: could not ensure tag schema: {e}")

        conn = sqlite3.connect(get_db_path())
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA temp_store = MEMORY;")

        try:
            # 1) اگر repaired_url پر است ولی is_invalid=0 است، پاک شود
            try:
                cur = conn.cursor()
                cur.execute(
                    """
                    UPDATE links
                       SET repaired_url = NULL
                     WHERE repaired_url IS NOT NULL
                       AND TRIM(repaired_url) <> ''
                       AND is_invalid = 0
                    """
                )
                conn.commit()
                with self._stats_lock:
                    self.stats["cleared_repaired_url"] += cur.rowcount if cur.rowcount > 0 else 0
            except Exception as e:
                print(f"[json_repair] WARN: cleanup repaired_url failed: {e}")

            last_id = 0

            while True:
                self._check_stopped()

                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT id, url, outbound_tag
                    FROM links
                    WHERE is_invalid = 1
                      AND is_protocol_unsupported = 0
                      AND url IS NOT NULL
                      AND TRIM(url) <> ''
                      AND id > ?
                    ORDER BY id
                    LIMIT ?
                    """,
                    (last_id, self.batch_size),
                )
                rows = cur.fetchall()

                if not rows:
                    print("[json_repair] no more candidates, exiting loop.")
                    break

                with self._stats_lock:
                    self.stats["batches"] += 1
                    self.stats["total_candidates"] += len(rows)

                updates_success: List[Tuple[str, int]] = []     # (config_json, id)
                updates_repaired: List[Tuple[str, int]] = []    # (repaired_url, id)
                mark_unsupported: List[int] = []                # ids

                for r in rows:
                    self._check_stopped()

                    row_id = int(r["id"])
                    last_id = row_id

                    url = str(r["url"] or "").strip()
                    if not url:
                        continue

                    try:
                        outbound_tag = self._ensure_outbound_tag_for_id(conn, row_id, r["outbound_tag"])
                    except Exception as e:
                        print(f"[json_repair] WARN could not ensure outbound_tag for id={row_id}: {e}")
                        with self._stats_lock:
                            self.stats["urls_failed"] += 1
                        continue

                    with self._stats_lock:
                        self.stats["urls_seen"] += 1

                    proto = self._detect_protocol(url)
                    if proto and proto.lower() not in self.SUPPORTED_PROTOCOLS:
                        # unsupported: invalid برداشته شود + unsupported ست شود
                        mark_unsupported.append(row_id)
                        continue

                    repaired, reason = self._repair_url(url)
                    if not repaired:
                        # هیچ repairی نشد → repaired_url همان clean (اگر fragment داشت) را می‌گذاریم
                        repaired = self._strip_fragment(url).strip()

                    # تلاش تبدیل repaired
                    json_text = self._convert_to_outbound_json_text(repaired, outbound_tag)
                    if json_text:
                        updates_success.append((json_text, row_id))
                        with self._stats_lock:
                            self.stats["urls_converted"] += 1
                            if repaired != url:
                                self.stats["urls_repaired"] += 1
                    else:
                        updates_repaired.append((repaired, row_id))
                        with self._stats_lock:
                            self.stats["urls_failed"] += 1

                # apply batch
                cur = conn.cursor()
                cur.execute("BEGIN")
                try:
                    if updates_success:
                        cur.executemany(
                            """
                            UPDATE links
                               SET config_json = ?,
                                   is_invalid = 0,
                                   repaired_url = NULL
                             WHERE id = ?
                            """,
                            updates_success,
                        )

                    if updates_repaired:
                        cur.executemany(
                            """
                            UPDATE links
                               SET repaired_url = ?,
                                   is_invalid = 1
                             WHERE id = ?
                            """,
                            updates_repaired,
                        )

                    if mark_unsupported:
                        cur.executemany(
                            """
                            UPDATE links
                               SET is_protocol_unsupported = 1,
                                   is_invalid = 0,
                                   repaired_url = NULL
                             WHERE id = ?
                            """,
                            [(i,) for i in mark_unsupported],
                        )
                        with self._stats_lock:
                            self.stats["marked_unsupported"] += len(mark_unsupported)

                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise

                print(
                    f"[json_repair] batch done: {len(rows)} rows, "
                    f"success={len(updates_success)}, still_invalid={len(updates_repaired)}, unsupported={len(mark_unsupported)}"
                )

        except JsonRepairStopped:
            print("[json_repair] stopped by request.")
        finally:
            try:
                conn.close()
            except Exception:
                pass

        print(
            "[json_repair] finished:"
            f"\n total_candidates: {self.stats['total_candidates']}"
            f"\n batches: {self.stats['batches']}"
            f"\n urls_seen: {self.stats['urls_seen']}"
            f"\n urls_repaired: {self.stats['urls_repaired']}"
            f"\n urls_converted: {self.stats['urls_converted']}"
            f"\n urls_failed: {self.stats['urls_failed']}"
            f"\n marked_unsupported: {self.stats['marked_unsupported']}"
            f"\n cleared_repaired_url: {self.stats['cleared_repaired_url']}"
        )


if __name__ == "__main__":
    JsonRepairUpdater(batch_size=1000).repair_and_fill_json()
