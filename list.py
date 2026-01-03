#!/usr/bin/env python3
import os

# مسیرهای اصلی
BASE_DIR = "/opt/xraymgr"
APP_DIR = os.path.join(BASE_DIR, "app")
DATA_DIR = os.path.join(BASE_DIR, "data")
DOC_DIR = os.path.join(BASE_DIR, "doc")
OUTPUT_FILE = os.path.join(BASE_DIR, "filelist.txt")

# پوشهٔ web_static برای فایل‌های فرانت‌اند پنل
WEB_STATIC_DIR = os.path.join(APP_DIR, "xraymgr", "web_static")

# مسیر اپلیکیشن داخل app که باید .py و .sql از آن لیست شود
APP_XRAYMGR_DIR = os.path.join(APP_DIR, "xraymgr")

# تنظیمات گیت‌هاب
GITHUB_USER = "damoon223"
GITHUB_REPO = "xraymgr"
GITHUB_BRANCH = "main"

# base URL برای tree و raw
GITHUB_TREE_BASE = f"https://github.com/{GITHUB_USER}/{GITHUB_REPO}/tree/{GITHUB_BRANCH}/"
GITHUB_RAW_BASE = f"https://raw.githubusercontent.com/{GITHUB_USER}/{GITHUB_REPO}/refs/heads/{GITHUB_BRANCH}/"

# مسیرهای نسبی‌ای که نباید لیست شوند (از BASE_DIR حساب می‌شوند)
EXCLUDE_DIRS = [
    ".git",
    "logs",
    "venv",
    "webbundle",
    os.path.join("app", "xraymgr", "__pycache__"),
]


def is_excluded(path: str) -> bool:
    """
    بر اساس مسیر کامل، تشخیص می‌دهد که این مسیر یا زیرشاخه‌هایش باید نادیده گرفته شوند یا نه.
    """
    rel = os.path.relpath(path, BASE_DIR)
    rel = rel.replace("\\", "/")

    for ex in EXCLUDE_DIRS:
        ex_norm = ex.replace("\\", "/")
        if rel == ex_norm or rel.startswith(ex_norm + "/"):
            return True
    return False


def main():
    # دیکشنری برای حذف موارد تکراری
    # key = مسیر نسبی از BASE_DIR، value = نوع ('d' یا 'f')
    entries: dict[str, str] = {}

    # 1) همه‌ی پوشه‌ها زیر /opt/xraymgr  (مثل find -type d)
    for root, dirs, files in os.walk(BASE_DIR):
        if is_excluded(root):
            dirs[:] = []
            continue

        dirs[:] = [d for d in dirs if not is_excluded(os.path.join(root, d))]

        rel_root = os.path.relpath(root, BASE_DIR)
        if rel_root == ".":
            entries["."] = "d"
        else:
            entries[rel_root.replace("\\", "/")] = "d"

    # 2) فقط فایل‌های .py و .sql در /opt/xraymgr/app/xraymgr و زیرشاخه‌ها
    if os.path.isdir(APP_XRAYMGR_DIR):
        for root, dirs, files in os.walk(APP_XRAYMGR_DIR):
            if is_excluded(root):
                dirs[:] = []
                continue
            dirs[:] = [d for d in dirs if not is_excluded(os.path.join(root, d))]

            for fname in files:
                full_path = os.path.join(root, fname)
                if is_excluded(full_path):
                    continue
                if fname.endswith((".py", ".sql")):
                    rel_path = os.path.relpath(full_path, BASE_DIR)
                    entries[rel_path.replace("\\", "/")] = "f"

    # 3) در پوشه‌ی data هر چی هست (فایل و پوشه)
    if os.path.isdir(DATA_DIR):
        for root, dirs, files in os.walk(DATA_DIR):
            if is_excluded(root):
                dirs[:] = []
                continue
            dirs[:] = [d for d in dirs if not is_excluded(os.path.join(root, d))]

            # خود دایرکتوری جاری
            rel_root = os.path.relpath(root, BASE_DIR)
            entries[rel_root.replace("\\", "/")] = "d"

            # فایل‌های داخلش
            for fname in files:
                full_path = os.path.join(root, fname)
                if is_excluded(full_path):
                    continue
                rel_path = os.path.relpath(full_path, BASE_DIR)
                entries[rel_path.replace("\\", "/")] = "f"

    # 4) فایل‌های front پنل در web_static (index.html / dashboard.js / dashboard.css)
    if os.path.isdir(WEB_STATIC_DIR):
        for fname in ("index.html", "dashboard.js", "dashboard.css"):
            full_path = os.path.join(WEB_STATIC_DIR, fname)
            if not os.path.isfile(full_path):
                continue
            if is_excluded(full_path):
                continue
            rel_path = os.path.relpath(full_path, BASE_DIR)
            entries[rel_path.replace("\\", "/")] = "f"

    # 5) فقط فایل‌های .txt و .text در /opt/xraymgr/doc و زیرشاخه‌ها
    if os.path.isdir(DOC_DIR):
        for root, dirs, files in os.walk(DOC_DIR):
            if is_excluded(root):
                dirs[:] = []
                continue
            dirs[:] = [d for d in dirs if not is_excluded(os.path.join(root, d))]

            for fname in files:
                full_path = os.path.join(root, fname)
                if is_excluded(full_path):
                    continue
                if fname.endswith((".txt", ".text")):
                    rel_path = os.path.relpath(full_path, BASE_DIR)
                    entries[rel_path.replace("\\", "/")] = "f"

    # تبدیل مسیر نسبی به URL گیت‌هاب
    def to_github_url(path: str, kind: str) -> str:
        if kind == "d":
            if path == ".":
                return GITHUB_TREE_BASE
            base = GITHUB_TREE_BASE
        else:
            base = GITHUB_RAW_BASE

        norm_path = path.replace("\\", "/").lstrip("/")
        return base + norm_path

    # نوشتن خروجی مرتب شده بر اساس مسیر
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for path in sorted(entries.keys()):
            kind = entries[path]
            url = to_github_url(path, kind)
            f.write(f"{kind} {url}\n")

    print(f"تمام شد. {len(entries)} مورد در فایل {OUTPUT_FILE} نوشته شد.")


if __name__ == "__main__":
    main()
