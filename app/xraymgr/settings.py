import os
from pathlib import Path

# روت پروژه: /opt/xraymgr
BASE_DIR = Path(__file__).resolve().parents[2]

# پوشه‌ی دیتا
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA_DIR / "xraymgr.db"

def get_db_path() -> str:
    """
    مسیر دیتابیس sqlite را برمی‌گرداند.
    """
    return str(DB_PATH)
