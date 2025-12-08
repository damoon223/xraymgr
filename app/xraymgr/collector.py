import base64
import json
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional

import requests

from .settings import BASE_DIR


PROTO_PREFIXES = (
    "vmess://",
    "vless://",
    "trojan://",
    "ss://",
    "ssr://",
    "tuic://",
    "hysteria2://",
    "hy2://",
)


class CollectorStopped(Exception):
    """Signal برای توقف graceful کالکتور."""
    pass


class SubscriptionCollector:
    """
    جمع‌آوری ساب‌ها:
      - خواندن URLهای ساب از فایل
      - دانلود محتوا
      - استخراج لینک‌های vmess/vless/trojan/ss/ssr/tuic/hysteria2/hy2
        * از متن معمولی
        * از blobهای base64
        * از JSONهای ساختاری
      - ذخیره در فایل raw_configs.txt
      - هر ساب که بعد از retry هم هیچ کانفیگی نده، از فایل sources حذف می‌شود.
      - قابلیت توقف میانه‌ی کار با request_stop()
    """

    def __init__(
        self,
        sources_file: Optional[str] = None,
        output_file: Optional[str] = None,
        max_workers: int = 10,
        timeout: int = 30,
    ) -> None:
        if sources_file is None:
            sources_file = os.path.join(
                BASE_DIR,
                "data",
                "sources",
                "proxy_sources.txt",
            )

        if output_file is None:
            output_file = os.path.join(
                BASE_DIR,
                "data",
                "raw",
                "raw_configs.txt",
            )

        self.sources_file = sources_file
        self.output_file = output_file
        self.max_workers = max_workers
        self.timeout = timeout

        os.makedirs(os.path.dirname(self.sources_file), exist_ok=True)
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0 Safari/537.36"
                )
            }
        )

        self.stats: Dict[str, int] = {
            "total_sources": 0,
            "successful_sources": 0,
            "failed_sources": 0,
            "total_configs": 0,
            "json_content_count": 0,
            "base64_content_count": 0,
            "text_content_count": 0,
            "empty_responses": 0,
            "retry_attempts": 0,
        }

        self.failed_urls: List[str] = []
        self.successful_urls: List[str] = []

        self._stats_lock = threading.Lock()
        self._sources_lock = threading.Lock()

        # Event برای توقف graceful
        self._stop_event = threading.Event()

    # ---------- کنترل توقف ----------

    def request_stop(self) -> None:
        """از بیرون صدا زده می‌شود تا کالکتور را متوقف کند."""
        print("[collector] stop requested")
        self._stop_event.set()

    def _check_stopped(self):
        if self._stop_event.is_set():
            raise CollectorStopped()

    # ---------- متدهای کمکی ----------

    def _read_sources(self) -> List[str]:
        if not os.path.exists(self.sources_file):
            print(f"[collector] sources file not found: {self.sources_file}")
            return []

        with open(self.sources_file, "r", encoding="utf-8") as f:
            links = [
                line.strip()
                for line in f
                if line.strip() and not line.lstrip().startswith("#")
            ]

        with self._stats_lock:
            self.stats["total_sources"] = len(links)

        print(f"[collector] read {len(links)} subscription URLs from {self.sources_file}")
        return links

    @staticmethod
    def _is_base64_encoded(text: str) -> bool:
        try:
            cleaned = re.sub(r"\s", "", text)
            if len(cleaned) % 4 != 0:
                return False
            base64.b64decode(cleaned, validate=True)
            return True
        except Exception:
            return False

    @staticmethod
    def _is_json_content(content: str) -> bool:
        try:
            json.loads(content)
            return True
        except Exception:
            return False

    @staticmethod
    def _safe_decode_base64(data: str) -> Optional[str]:
        try:
            s = data.strip()
            s = re.sub(r"\s+", "", s)
            missing_padding = len(s) % 4
            if missing_padding:
                s += "=" * (4 - missing_padding)
            return base64.b64decode(s).decode("utf-8", errors="ignore")
        except Exception:
            return None

    def _decode_if_base64(self, content: str) -> str:
        if self._is_base64_encoded(content):
            decoded = self._safe_decode_base64(content)
            if decoded is not None:
                return decoded
        return content

    def _extract_configs_from_text(self, content: str) -> List[str]:
        if not content:
            return []

        content = self._decode_if_base64(content)

        patterns = [
            r"vmess://[A-Za-z0-9+/=]+",
            r"vless://[^\s\n]+",
            r"trojan://[^\s\n]+",
            r"ss://[A-Za-z0-9+/=]+@[^\s\n]+",
            r"ssr://[A-Za-z0-9+/=]+",
            r"tuic://[^\s\n]+",
            r"hysteria2://[^\s\n]+",
            r"hy2://[^\s\n]+",
        ]

        configs: List[str] = []
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            configs.extend(matches)

        return list(set(configs))

    def _extract_configs_from_base64_blob(self, blob: str) -> List[str]:
        decoded = self._safe_decode_base64(blob)
        if not decoded:
            return []

        configs: List[str] = []
        for line in decoded.splitlines():
            line = line.strip()
            if not line:
                continue
            if line.startswith(PROTO_PREFIXES):
                configs.append(line)

        return list(set(configs))

    # ---- استخراج از JSON (بر اساس parser.py قدیمی) ----

    def _convert_structured_to_url(self, outbound: dict) -> Optional[str]:
        try:
            if not isinstance(outbound, dict):
                return None
            config_type = outbound.get("type", "").lower()
            if config_type == "hysteria2":
                server = outbound.get("server", "")
                server_port = outbound.get("server_port", 443)
                password = outbound.get("password", "")
                tag = outbound.get("tag", "")
                if server and password:
                    url = f"hysteria2://{password}@{server}:{server_port}"
                    if tag:
                        url += f"#{tag}"
                    return url
            elif config_type == "wireguard":
                settings = outbound.get("settings", {})
                peers = settings.get("peers", [])
                if peers and len(peers) > 0:
                    endpoint = peers[0].get("endpoint", "")
                    public_key = peers[0].get("publicKey", "")
                    tag = outbound.get("tag", "")
                    if endpoint and public_key:
                        return f"# Wireguard config: {tag} - {endpoint}"
            return None
        except Exception:
            return None

    def _extract_configs_from_json_obj(self, json_obj) -> List[str]:
        configs: List[str] = []

        def search(obj):
            if isinstance(obj, dict):
                if "outbounds" in obj and isinstance(obj["outbounds"], list):
                    for outbound in obj["outbounds"]:
                        if isinstance(outbound, dict):
                            url = self._convert_structured_to_url(outbound)
                            if url:
                                configs.append(url)

                for _, value in obj.items():
                    if isinstance(value, str) and value.startswith(PROTO_PREFIXES):
                        configs.append(value)
                    elif isinstance(value, (dict, list)):
                        search(value)

            elif isinstance(obj, list):
                for item in obj:
                    search(item)

        search(json_obj)
        return list(set(configs))

    def _extract_configs_from_json_text(self, content: str) -> List[str]:
        try:
            data = json.loads(content)
        except Exception:
            return []

        return self._extract_configs_from_json_obj(data)

    # ---------- HTTP ----------

    def _fetch_url_with_retry(
        self,
        url: str,
        index: int,
        total: int,
        max_retries: int = 3,
    ) -> Optional[str]:
        for attempt in range(max_retries):
            self._check_stopped()
            try:
                if attempt == 0:
                    print(f"[collector] ({index}/{total}) fetching: {url}")
                else:
                    print(f"[collector] ({index}/{total}) retry {attempt+1} for: {url}")

                resp = self.session.get(url, timeout=self.timeout)
                resp.raise_for_status()
                text = resp.text
                if not text.strip():
                    with self._stats_lock:
                        self.stats["empty_responses"] += 1
                    if attempt < max_retries - 1:
                        with self._stats_lock:
                            self.stats["retry_attempts"] += 1
                        time.sleep(1.0)
                        continue
                    print(f"[collector] ({index}/{total}) empty response, giving up")
                    return None
                return text
            except requests.RequestException as e:
                self._check_stopped()
                if attempt < max_retries - 1:
                    with self._stats_lock:
                        self.stats["retry_attempts"] += 1
                    print(f"[collector] ({index}/{total}) error: {e.__class__.__name__}, retrying...")
                    time.sleep(1.0)
                    continue
                else:
                    print(f"[collector] ({index}/{total}) FAILED after retries: {url}")
                    return None
            except CollectorStopped:
                raise
            except Exception as e:
                print(f"[collector] ({index}/{total}) unexpected error: {str(e)[:80]}")
                return None
        return None

    def _remove_source_from_file(self, url: str) -> None:
        try:
            if not os.path.exists(self.sources_file):
                return

            with self._sources_lock:
                with open(self.sources_file, "r", encoding="utf-8") as f:
                    lines = f.readlines()

                new_lines: List[str] = []
                removed = False

                for line in lines:
                    stripped = line.strip()
                    if not stripped or stripped.lstrip().startswith("#"):
                        new_lines.append(line)
                        continue
                    if stripped == url and not removed:
                        removed = True
                        continue
                    new_lines.append(line)

                if removed:
                    with open(self.sources_file, "w", encoding="utf-8") as f:
                        f.writelines(new_lines)
                    print(f"[collector] removed bad source from file: {url}")
        except Exception as e:
            print(f"[collector] error removing bad source {url}: {e}")

    # ---------- پردازش هر ساب ----------

    def _process_single_source(self, url: str, index: int, total: int) -> List[str]:
        self._check_stopped()

        content = self._fetch_url_with_retry(url, index=index, total=total)
        if self._stop_event.is_set():
            # اگر وسط کار stop شده، بدون دست‌زدن به آمار، فقط خارج شو
            raise CollectorStopped()

        if not content:
            with self._stats_lock:
                self.stats["failed_sources"] += 1
                self.failed_urls.append(url)
            self._remove_source_from_file(url)
            return []

        configs: List[str] = []

        # 1) JSON
        if self._is_json_content(content):
            json_configs = self._extract_configs_from_json_text(content)
            if json_configs:
                with self._stats_lock:
                    self.stats["successful_sources"] += 1
                    self.stats["json_content_count"] += 1
                    self.stats["total_configs"] += len(json_configs)
                    self.successful_urls.append(url)
                print(f"[collector] ({index}/{total}) JSON content → {len(json_configs)} configs")
                configs.extend(json_configs)
            else:
                with self._stats_lock:
                    self.stats["failed_sources"] += 1
                    self.failed_urls.append(url)
                print(f"[collector] ({index}/{total}) JSON content without configs, removing source")
                self._remove_source_from_file(url)
            return configs

        # 2) base64 blob
        if self._is_base64_encoded(content):
            b64_configs = self._extract_configs_from_base64_blob(content)
            if b64_configs:
                with self._stats_lock:
                    self.stats["successful_sources"] += 1
                    self.stats["base64_content_count"] += 1
                    self.stats["total_configs"] += len(b64_configs)
                    self.successful_urls.append(url)
                print(f"[collector] ({index}/{total}) base64 blob → {len(b64_configs)} configs")
                configs.extend(b64_configs)
            else:
                with self._stats_lock:
                    self.stats["failed_sources"] += 1
                    self.failed_urls.append(url)
                print(f"[collector] ({index}/{total}) base64 content without configs, removing source")
                self._remove_source_from_file(url)
            return configs

        # 3) متن معمولی
        text_configs = self._extract_configs_from_text(content)
        if text_configs:
            with self._stats_lock:
                self.stats["successful_sources"] += 1
                self.stats["text_content_count"] += 1
                self.stats["total_configs"] += len(text_configs)
                self.successful_urls.append(url)
            print(f"[collector] ({index}/{total}) text content → {len(text_configs)} configs")
            configs.extend(text_configs)
        else:
            with self._stats_lock:
                self.stats["failed_sources"] += 1
                self.failed_urls.append(url)
            print(f"[collector] ({index}/{total}) no configs found in text, removing source")
            self._remove_source_from_file(url)

        return configs

    # ---------- رابط عمومی ----------

    def collect_from_sources_file(self) -> List[str]:
        sources = self._read_sources()
        if not sources:
            print("[collector] no sources to process")
            return []

        all_configs: List[str] = []

        total = len(sources)
        print(
            f"[collector] starting collection from {total} sources "
            f"using {self.max_workers} workers..."
        )

        indexed_sources = list(enumerate(sources, start=1))

        try:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_meta = {
                    executor.submit(self._process_single_source, url, idx, total): (idx, url)
                    for idx, url in indexed_sources
                }
                for future in as_completed(future_to_meta):
                    idx, url = future_to_meta[future]
                    try:
                        configs = future.result()
                        if configs:
                            all_configs.extend(configs)
                    except CollectorStopped:
                        # توقف graceful: بقیه futureها هم به‌مرور خارج می‌شوند
                        print(f"[collector] stop signal received, aborting remaining work...")
                        break
                    except Exception as e:
                        print(f"[collector] ({idx}/{total}) worker error: {e}")
                        with self._stats_lock:
                            self.stats["failed_sources"] += 1
                            self.failed_urls.append(url)
                        # اینجا stop نیست، پس منبع خراب واقعاً خراب است
                        self._remove_source_from_file(url)
        except CollectorStopped:
            print("[collector] stopped by request")

        print(
            "\n[collector] collection finished:"
            f"\n   total sources:       {self.stats['total_sources']}"
            f"\n   successful sources:  {self.stats['successful_sources']}"
            f"\n   failed sources:      {self.stats['failed_sources']}"
            f"\n   configs found (raw): {len(all_configs)}"
            f"\n   json contents:       {self.stats['json_content_count']}"
            f"\n   base64 contents:     {self.stats['base64_content_count']}"
            f"\n   text contents:       {self.stats['text_content_count']}"
            f"\n   empty responses:     {self.stats['empty_responses']}"
            f"\n   retry attempts:      {self.stats['retry_attempts']}"
        )

        self._save_configs_to_file(all_configs)

        if self.failed_urls:
            print("\n[collector] failed URLs (also removed from sources file):")
            for i, url in enumerate(self.failed_urls, start=1):
                print(f"   {i:3d}. {url}")

        return all_configs

    def _save_configs_to_file(self, configs: List[str]) -> None:
        if not configs:
            print("[collector] no configs to save")
            return

        try:
            os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
            with open(self.output_file, "w", encoding="utf-8") as f:
                for cfg in configs:
                    f.write(cfg + "\n")
            print(
                f"[collector] saved {len(configs)} configs to {self.output_file}"
            )
        except Exception as e:
            print(f"[collector] error saving configs: {e}")


if __name__ == "__main__":
    print("XrayMgr Subscription Collector")
    print("=" * 32)
    collector = SubscriptionCollector()
    try:
        configs = collector.collect_from_sources_file()
        print(f"\nCollected {len(configs)} configs.")
    except CollectorStopped:
        print("\nCollector stopped by user.")
