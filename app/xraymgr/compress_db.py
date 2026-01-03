#!/usr/bin/env python3
import os
import sys
import time
import lzma

# --- تنظیم sys.path برای اینکه بتوانیم xraymgr.settings را ایمپورت کنیم ---

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
APP_DIR = os.path.dirname(CURRENT_DIR)  # /opt/xraymgr/app

if APP_DIR not in sys.path:
    sys.path.insert(0, APP_DIR)

from xraymgr.settings import get_db_path  # حالا import مطمئن کار می‌کند


def compress_db(db_path: str, out_path: str) -> None:
    if not os.path.exists(db_path):
        print(f"[compress] database file not found: {db_path}")
        sys.exit(1)

    total_size = os.path.getsize(db_path)
    if total_size == 0:
        print("[compress] database file is empty, nothing to do.")
        sys.exit(0)

    # حداکثر کامپرس: preset=9 | PRESET_EXTREME
    preset = 9 | lzma.PRESET_EXTREME

    print(f"[compress] input : {db_path}")
    print(f"[compress] output: {out_path}")
    print(f"[compress] size  : {total_size / (1024 * 1024):.2f} MiB")
    print(f"[compress] method: xz (lzma), preset={preset}")
    print()

    compressor = lzma.LZMACompressor(format=lzma.FORMAT_XZ, preset=preset)

    chunk_size = 1024 * 1024  # 1 MiB
    read_bytes = 0
    last_percent = -1
    start_time = time.time()

    with open(db_path, "rb") as f_in, open(out_path, "wb") as f_out:
        while True:
            chunk = f_in.read(chunk_size)
            if not chunk:
                break

            read_bytes += len(chunk)
            data = compressor.compress(chunk)
            if data:
                f_out.write(data)

            percent = int(read_bytes * 100 / total_size)
            if percent != last_percent:
                elapsed = time.time() - start_time
                speed = (read_bytes / (1024 * 1024)) / elapsed if elapsed > 0 else 0.0
                msg = (
                    f"\r[compress] {percent:3d}% | "
                    f"{read_bytes / (1024 * 1024):.1f} / {total_size / (1024 * 1024):.1f} MiB "
                    f"| {speed:.2f} MiB/s"
                )
                sys.stdout.write(msg)
                sys.stdout.flush()
                last_percent = percent

        # فلش نهایی
        data = compressor.flush()
        if data:
            f_out.write(data)

    sys.stdout.write("\n")

    out_size = os.path.getsize(out_path)
    ratio = (out_size / total_size) * 100.0
    print("[compress] done.")
    print(f"[compress] original size : {total_size / (1024 * 1024):.2f} MiB")
    print(f"[compress] compressed size: {out_size / (1024 * 1024):.2f} MiB")
    print(f"[compress] ratio         : {ratio:.1f}% of original")


def main() -> None:
    # مسیر دیتابیس را از تنظیمات می‌گیریم
    db_path = get_db_path()
    db_path = os.path.abspath(db_path)

    db_dir = os.path.dirname(db_path)
    db_base = os.path.basename(db_path)

    ts = time.strftime("%Y%m%d-%H%M%S")
    # خروجی در همان پوشهٔ دیتابیس، با اسم base + timestamp
    out_name = f"{db_base}.{ts}.xz"
    out_path = os.path.join(db_dir, out_name)

    compress_db(db_path, out_path)


if __name__ == "__main__":
    main()
