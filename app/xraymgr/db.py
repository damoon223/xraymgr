import sqlite3
from contextlib import contextmanager
from .settings import get_db_path

def get_connection() -> sqlite3.Connection:
    """
    یک اتصال sqlite3 با تنظیمات معقول برمی‌گرداند.
    """
    conn = sqlite3.connect(
        get_db_path(),
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
    )
    conn.row_factory = sqlite3.Row  # خروجی کوئری‌ها را dict مانند می‌کند
    return conn

@contextmanager
def db_cursor():
    """
    کانتکست منیجر برای گرفتن cursor و commit/rollback خودکار.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
