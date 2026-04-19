"""SQLite connection factory and session context manager."""

import sqlite3
from collections.abc import Generator
from contextlib import contextmanager

from bakery_analyst.config import settings


def get_connection(db_path: str | None = None) -> sqlite3.Connection:
    """Open a new SQLite connection with ``Row`` factory and WAL journal mode."""
    path = db_path or settings.db_path
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


@contextmanager
def db_session(db_path: str | None = None) -> Generator[sqlite3.Connection, None, None]:
    """Context manager: yield an open connection, commit on success, rollback on error."""
    conn = get_connection(db_path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
