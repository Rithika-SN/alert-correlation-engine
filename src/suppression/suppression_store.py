from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "data" / "generated" / "suppression.sqlite3"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS suppression_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            alert_id TEXT NOT NULL,
            reason TEXT NOT NULL,
            suppressed_at TEXT NOT NULL,
            reversible INTEGER NOT NULL DEFAULT 1,
            reversed INTEGER NOT NULL DEFAULT 0,
            reversed_at TEXT
        )
        """
    )
    conn.commit()
    return conn


def log_suppression(alert_id: str, reason: str, reversible: bool = True) -> dict:
    conn = _connect()
    row = {
        "alert_id": str(alert_id),
        "reason": str(reason),
        "suppressed_at": pd.Timestamp.utcnow().isoformat(),
        "reversible": int(reversible),
    }
    conn.execute(
        "INSERT INTO suppression_log (alert_id, reason, suppressed_at, reversible) VALUES (?, ?, ?, ?)",
        (row["alert_id"], row["reason"], row["suppressed_at"], row["reversible"]),
    )
    conn.commit()
    conn.close()
    return row


def read_suppressions() -> pd.DataFrame:
    conn = _connect()
    df = pd.read_sql_query(
        "SELECT alert_id, reason, suppressed_at, reversible, reversed, reversed_at FROM suppression_log ORDER BY suppressed_at DESC",
        conn,
    )
    conn.close()
    if df.empty:
        return pd.DataFrame(columns=["alert_id", "reason", "suppressed_at", "reversible", "reversed", "reversed_at"])
    df["reversible"] = df["reversible"].astype(bool)
    df["reversed"] = df["reversed"].astype(bool)
    return df


def reverse_suppression(alert_id: str) -> bool:
    conn = _connect()
    cur = conn.execute(
        "UPDATE suppression_log SET reversed = 1, reversed_at = ? WHERE alert_id = ? AND reversed = 0",
        (pd.Timestamp.utcnow().isoformat(), str(alert_id)),
    )
    conn.commit()
    conn.close()
    return cur.rowcount > 0


def get_suppression_count() -> int:
    conn = _connect()
    count = conn.execute("SELECT COUNT(*) FROM suppression_log WHERE reversed = 0").fetchone()[0]
    conn.close()
    return int(count)
