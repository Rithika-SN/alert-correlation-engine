from __future__ import annotations

import pandas as pd

FLAP_RESOLUTION_SECONDS = 60
FLAP_COUNT_THRESHOLD = 3
FLAP_WINDOW_SECONDS = 10 * 60


def detect_flapping_alerts(alerts: pd.DataFrame) -> pd.DataFrame:
    if alerts.empty:
        return pd.DataFrame(columns=["alert_id", "rule_id", "host", "window_count"])

    df = alerts.copy()
    df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
    df["resolved_ts"] = pd.to_datetime(df["resolved_ts"], errors="coerce")
    df["resolve_seconds"] = (df["resolved_ts"] - df["ts"]).dt.total_seconds()
    candidates = df[(df["resolved_ts"].notna()) & (df["resolve_seconds"] < FLAP_RESOLUTION_SECONDS)].copy()
    if candidates.empty:
        return pd.DataFrame(columns=["alert_id", "rule_id", "host", "window_count"])

    results: list[dict[str, object]] = []
    for (rule_id, host), group in candidates.groupby(["rule_id", "host"], sort=True):
        group = group.sort_values("ts").reset_index(drop=True)
        for _, row in group.iterrows():
            window_start = row["ts"] - pd.Timedelta(seconds=FLAP_WINDOW_SECONDS)
            window_end = row["ts"] + pd.Timedelta(seconds=FLAP_WINDOW_SECONDS)
            count = int(group[(group["ts"] >= window_start) & (group["ts"] <= window_end)].shape[0])
            if count >= FLAP_COUNT_THRESHOLD:
                results.append(
                    {
                        "alert_id": row["alert_id"],
                        "rule_id": str(rule_id),
                        "host": str(host),
                        "window_count": count,
                    }
                )

    if not results:
        return pd.DataFrame(columns=["alert_id", "rule_id", "host", "window_count"])

    frame = pd.DataFrame(results).drop_duplicates(subset=["alert_id"]).reset_index(drop=True)
    return frame


def suppress_flapping_alerts(alerts: pd.DataFrame, flapping_alerts: pd.DataFrame | list[str] | None = None):
    from src.suppression.suppression_store import log_suppression

    if flapping_alerts is None:
        return []

    if isinstance(flapping_alerts, pd.DataFrame):
        alert_ids = [str(alert_id) for alert_id in flapping_alerts["alert_id"].tolist() if pd.notna(alert_id)]
    else:
        alert_ids = [str(alert_id) for alert_id in flapping_alerts]

    unique_ids = sorted(set(alert_ids))
    suppressed = []
    for alert_id in unique_ids:
        log_suppression(alert_id, reason="flapping", reversible=True)
        suppressed.append(alert_id)
    return suppressed
