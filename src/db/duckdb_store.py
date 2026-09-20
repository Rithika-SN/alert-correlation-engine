from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
ALERTS_PATH = ROOT / "data" / "generated" / "alerts.parquet"


def connect() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(database=':memory:')


def top_noisy_rules(limit: int = 10) -> pd.DataFrame:
    if not ALERTS_PATH.exists():
        return pd.DataFrame(columns=["rule_id", "alert_count"])
    conn = connect()
    df = conn.sql(f"SELECT rule_id, COUNT(*) AS alert_count FROM read_parquet('{ALERTS_PATH.as_posix()}') GROUP BY rule_id ORDER BY alert_count DESC LIMIT {limit}").df()
    conn.close()
    return df


def alert_volume_over_time() -> pd.DataFrame:
    if not ALERTS_PATH.exists():
        return pd.DataFrame(columns=["ts", "alert_count"])
    conn = connect()
    df = conn.sql(f"SELECT CAST(ts AS DATE) AS ts, COUNT(*) AS alert_count FROM read_parquet('{ALERTS_PATH.as_posix()}') GROUP BY CAST(ts AS DATE) ORDER BY ts").df()
    conn.close()
    return df
