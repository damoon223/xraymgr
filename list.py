#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from pathlib import Path
from typing import Iterable

OWNER = "damoon223"
REPO = "xraymgr"
BRANCH = "main"

TREE_BASE = f"https://github.com/{OWNER}/{REPO}/tree/{BRANCH}"
RAW_BASE = f"https://raw.githubusercontent.com/{OWNER}/{REPO}/refs/heads/{BRANCH}"

# فقط همین ریشه‌ها را لیست می‌کنیم (مطابق خروجی‌ای که قبلاً دادی)
INCLUDE_ROOTS = [
    "app",
    "data",
    "doc",
    "scripts/install.sh",  # فقط همین فایل لازم است
]

# این‌ها را نمی‌خواهی
EXCLUDE_DIR_PREFIXES = [
    ".venv/",
    "data/sources/",
    "data/raw/",
]

# نویزهای رایج
EXCLUDE_DIR_NAMES = {".git", "__pycache__"}
EXCLUDE_FILE_NAMES = {"filelist.txt"}  # خودِ خروجی را داخل لیست نیاور


def _to_posix(rel: Path) -> str:
    return rel.as_posix().lstrip("./")


def _is_excluded_dir(rel_posix: str) -> bool:
    if any(rel_posix.startswith(p) for p in EXCLUDE_DIR_PREFIXES):
        return True
    # هر سگمنت اگر یکی از نام‌های نویز بود
    parts = rel_posix.split("/")
    return any(p in EXCLUDE_DIR_NAMES for p in parts if p)


def _walk_dirs_and_files(root: Path) -> tuple[list[str], list[str]]:
    """
    برمی‌گرداند:
      - dirs: لیست مسیرهای دایرکتوری (نسبت به repo root) به شکل posix
      - files: لیست مسیرهای فایل (نسبت به repo root) به شکل posix
    """
    dirs: list[str] = []
    files: list[str] = []

    repo_root = Path(__file__).resolve().parent

    if root.is_file():
        rel = root.relative_to(repo_root)
        rel_posix = _to_posix(rel)
        if Path(rel_posix).name not in EXCLUDE_FILE_NAMES:
            files.append(rel_posix)
        return dirs, files

    # دایرکتوری
    for p in root.rglob("*"):
        try:
            rel = p.relative_to(repo_root)
        except ValueError:
            continue

        rel_posix = _to_posix(rel)

        # فیلتر دایرکتوری‌های ناخواسته
        if p.is_dir():
            if _is_excluded_dir(rel_posix + "/"):
                continue
            dirs.append(rel_posix)
            continue

        # فایل
        if p.is_file():
            # اگر داخل مسیرهای حذف‌شده افتاد، رد کن
            parent_posix = _to_posix(rel.parent)
            if parent_posix and _is_excluded_dir(parent_posix + "/"):
                continue
            if p.name in EXCLUDE_FILE_NAMES:
                continue
            files.append(rel_posix)

    # مرتب‌سازی پایدار
    dirs = sorted(set(dirs))
    files = sorted(set(files))
    return dirs, files


def main() -> int:
    repo_root = Path(__file__).resolve().parent

    all_dirs: list[str] = []
    all_files: list[str] = []

    for item in INCLUDE_ROOTS:
        target = repo_root / item
        if not target.exists():
            continue

        dirs, files = _walk_dirs_and_files(target)

        # اگر خودِ ریشه یک دایرکتوری است، خودش را هم اضافه کن
        if target.is_dir():
            rel_root = _to_posix(target.relative_to(repo_root))
            if rel_root and not _is_excluded_dir(rel_root + "/"):
                all_dirs.append(rel_root)

        all_dirs.extend(dirs)
        all_files.extend(files)

    all_dirs = sorted(set(all_dirs))
    all_files = sorted(set(all_files))

    lines: list[str] = []
    for d in all_dirs:
        lines.append(f"d {TREE_BASE}/{d}")
    for f in all_files:
        lines.append(f"f {RAW_BASE}/{f}")

    # خروجی دقیقاً در همان جایی که اسکریپت اجرا می‌شود (CWD)
    out_path = Path.cwd() / "filelist.txt"
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(str(out_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
