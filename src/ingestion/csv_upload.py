from __future__ import annotations

import csv
import io
import re
from difflib import get_close_matches
from typing import Any

import pandas as pd

TIMESTAMP_HINTS = ("ts", "timestamp", "time", "date", "created", "occurred", "resolved")


def parse_uploaded_csv(file: Any) -> pd.DataFrame:
    raw = file.read() if hasattr(file, "read") else file
    if isinstance(raw, bytes):
        text = raw.decode("utf-8-sig", errors="replace")
    else:
        text = str(raw)

    sample = text[:4096]
    delimiter = ","
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
        delimiter = dialect.delimiter
    except Exception:
        delimiter = "," if "," in sample else ";" if ";" in sample else "\t" if "\t" in sample else ","

    try:
        df = pd.read_csv(io.StringIO(text), sep=delimiter, engine="python")
    except Exception:
        df = pd.read_csv(io.StringIO(text), sep=delimiter, engine="c")
    return df


def detect_timestamp_candidates(df: pd.DataFrame) -> list[str]:
    candidates: set[str] = set()
    for column in df.columns:
        lowered = str(column).lower()
        if any(token in lowered for token in TIMESTAMP_HINTS):
            candidates.add(str(column))

    for column in df.columns:
        series = df[column].dropna()
        if series.empty:
            continue
        parsed = pd.to_datetime(series.head(20), errors="coerce")
        if len(parsed) > 0 and parsed.notna().mean() > 0.9:
            candidates.add(str(column))
    return list(candidates)


def suggest_column_mapping(df: pd.DataFrame, required: list[str]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for target in required:
        close_matches = get_close_matches(target.lower(), [str(col).lower() for col in df.columns], n=1, cutoff=0.2)
        if close_matches:
            source = df.columns[[str(col).lower() for col in df.columns].index(close_matches[0])]
            mapping[target] = str(source)
    return mapping


def normalize_uploaded_alerts(df: pd.DataFrame, ts_col: str | None = None, resolved_col: str | None = None, service_col: str | None = None, rule_col: str | None = None, summary_col: str | None = None, severity_col: str | None = None) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["alert_id", "ts", "service", "host", "severity", "rule_id", "summary", "labels_json", "resolved_ts"])

    normalized = df.copy()
    ts_column = ts_col or detect_timestamp_candidates(df)[0] if detect_timestamp_candidates(df) else None
    if ts_column is None:
        raise ValueError("No timestamp column could be detected for the uploaded CSV.")

    normalized["ts"] = pd.to_datetime(normalized[ts_column], errors="coerce")
    normalized["resolved_ts"] = pd.to_datetime(normalized[resolved_col], errors="coerce") if resolved_col and resolved_col in normalized.columns else pd.NaT

    service_column = service_col or ("service" if "service" in normalized.columns else next((c for c in normalized.columns if "service" in str(c).lower()), None))
    rule_column = rule_col or ("rule_id" if "rule_id" in normalized.columns else next((c for c in normalized.columns if "rule" in str(c).lower()), None))
    summary_column = summary_col or ("summary" if "summary" in normalized.columns else next((c for c in normalized.columns if "summary" in str(c).lower() or "message" in str(c).lower()), None))
    severity_column = severity_col or ("severity" if "severity" in normalized.columns else None)

    if service_column is None or rule_column is None:
        raise ValueError("Uploaded CSV must include at least service and rule_id columns after mapping.")

    normalized["service"] = normalized[service_column].fillna("unknown-service").astype(str)
    normalized["rule_id"] = normalized[rule_column].fillna("unknown-rule").astype(str)
    normalized["summary"] = normalized[summary_column].fillna(normalized[service_column].astype(str) + " alert") if summary_column and summary_column in normalized.columns else "Alert summary unavailable"
    normalized["severity"] = normalized[severity_column].fillna("medium").astype(str).str.lower() if severity_column and severity_column in normalized.columns else "medium"
    normalized["host"] = normalized.get("host", "unknown-host").fillna("unknown-host").astype(str) if "host" in normalized.columns else "unknown-host"

    normalized["alert_id"] = [f"upload_{idx:06d}" for idx in range(len(normalized))]
    normalized["labels_json"] = "{}"
    required_cols = ["alert_id", "ts", "service", "host", "severity", "rule_id", "summary", "labels_json", "resolved_ts"]
    return normalized[required_cols]
