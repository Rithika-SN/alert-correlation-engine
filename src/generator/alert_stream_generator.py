from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

SERVICE_NAMES = [
    "auth", "billing", "checkout", "catalog", "inventory", "orders", "search",
    "recommendations", "payments", "fraud", "shipping", "warehouse", "user-profile",
    "notifications", "gateway",
]


def _synthetic_summary(service: str, rule_id: str, severity: str, host: str) -> str:
    issue_map = {
        "critical": "critical degradation",
        "high": "high latency",
        "medium": "intermittent error burst",
        "low": "warning signal",
    }
    return f"{service} {issue_map.get(severity, 'warning signal')} on {host}; rule {rule_id} triggered unexpected failures"


def _build_topology(services: list[str]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for idx, service in enumerate(services):
        upstream_candidates = services[:max(0, idx - 2)]
        for upstream in upstream_candidates[:3]:
            rows.append(
                {
                    "from_service": upstream,
                    "to_service": service,
                    "call_rate": round(0.6 + (idx % 5) * 0.2 + (len(upstream) % 3) * 0.05, 3),
                }
            )
    if not rows:
        rows = [{"from_service": services[0], "to_service": services[1], "call_rate": 1.0}]
    return pd.DataFrame(rows)


def _build_maintenance_windows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"service": "billing", "start": "2024-01-01 12:00:00", "end": "2024-01-01 13:30:00", "scope": "billing-db"},
            {"service": "checkout", "start": "2024-01-02 06:00:00", "end": "2024-01-02 08:00:00", "scope": "cache"},
            {"service": "shipping", "start": "2024-01-03 15:15:00", "end": "2024-01-03 17:00:00", "scope": "pickup-circuit"},
        ]
    )


def generate_alert_dataset(output_dir: str = "data/generated", scale: str = "demo", seed: int = 42):
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)

    if scale == "demo":
        total_alerts = 5000
        days = 3
        services = SERVICE_NAMES[:15]
    elif scale == "full":
        total_alerts = 200_000
        days = 30
        services = [f"svc_{i:02d}" for i in range(50)]
    else:
        raise ValueError("scale must be 'demo' or 'full'")

    rng = np.random.default_rng(seed)
    start_ts = pd.Timestamp("2024-01-01 00:00:00")
    service_order = sorted(services)
    topology = _build_topology(service_order)
    maintenance = _build_maintenance_windows()

    rule_ids = [f"rule_{i:02d}" for i in range(1, 15)]
    severity_levels = ["low", "medium", "high", "critical"]
    severity_weights = [0.25, 0.35, 0.25, 0.15]

    storm_windows = [
        (start_ts + pd.Timedelta(hours=8), "billing", "gateway"),
        (start_ts + pd.Timedelta(hours=30), "checkout", "orders"),
        (start_ts + pd.Timedelta(hours=52), "shipping", "inventory"),
    ]

    incident_rows: list[dict[str, object]] = []
    for idx, (storm_start, root_service, down_service) in enumerate(storm_windows):
        for offset in range(25):
            local_ts = storm_start + pd.Timedelta(minutes=offset)
            for svc in [root_service, down_service] + list(service_order[:5]):
                if svc == root_service:
                    rule_id = "rule_01"
                    severity = "critical"
                elif svc == down_service:
                    rule_id = "rule_02"
                    severity = "high"
                else:
                    rule_id = f"rule_{(idx % 10) + 3:02d}"
                    severity = "medium"
                host = f"host-{svc}-{idx + 1}-{offset % 5}"
                alert_id = f"storm_{idx}_{offset}_{svc}"
                summary = _synthetic_summary(svc, rule_id, severity, host)
                resolved = local_ts + pd.Timedelta(minutes=2 if severity == "critical" else 5)
                incident_rows.append(
                    {
                        "alert_id": alert_id,
                        "ts": local_ts,
                        "service": svc,
                        "host": host,
                        "severity": severity,
                        "rule_id": rule_id,
                        "summary": summary,
                        "labels_json": json.dumps({"incident_id": f"storm_{idx}"}),
                        "resolved_ts": resolved,
                    }
                )

    flap_rows: list[dict[str, object]] = []
    flap_hosts = ["host-flap-01", "host-flap-02"]
    for idx, host in enumerate(flap_hosts):
        base_ts = start_ts + pd.Timedelta(hours=5 + idx * 4)
        for cycle in range(5):
            ts = base_ts + pd.Timedelta(minutes=cycle * 2)
            duration = pd.Timedelta(seconds=25 + cycle * 2)
            alert_id = f"flap_{idx}_{cycle}"
            flap_rows.append(
                {
                    "alert_id": alert_id,
                    "ts": ts,
                    "service": service_order[(idx + cycle) % len(service_order)],
                    "host": host,
                    "severity": "medium",
                    "rule_id": "rule_10",
                    "summary": f"{host} flap event due to transient network blip",
                    "labels_json": json.dumps({"incident_id": f"flap_{idx}"}),
                    "resolved_ts": ts + duration,
                }
            )

    regular_rows = []
    for i in range(total_alerts):
        hour_offset = rng.integers(0, int(days * 24))
        minute_offset = rng.integers(0, 60)
        ts = start_ts + pd.Timedelta(hours=hour_offset, minutes=minute_offset)
        service = service_order[rng.integers(0, len(service_order))]
        host = f"host-{service}-{rng.integers(1, 12)}"
        severity = rng.choice(severity_levels, p=severity_weights)
        rule_id = rule_ids[rng.integers(0, len(rule_ids))]
        if rng.random() < 0.15:
            rule_id = "rule_01"
        summary = _synthetic_summary(service, rule_id, severity, host)
        resolved = ts + pd.Timedelta(minutes=rng.integers(1, 15))
        regular_rows.append(
            {
                "alert_id": f"alert_{i:06d}",
                "ts": ts,
                "service": service,
                "host": host,
                "severity": severity,
                "rule_id": rule_id,
                "summary": summary,
                "labels_json": json.dumps({"incident_id": f"noise_{i % 50}"}),
                "resolved_ts": resolved,
            }
        )

    frame = pd.DataFrame(incident_rows + flap_rows + regular_rows)
    frame = frame.sort_values("ts").reset_index(drop=True)
    frame["alert_id"] = frame["alert_id"].astype(str)
    frame["severity"] = frame["severity"].astype(str)
    frame["rule_id"] = frame["rule_id"].astype(str)
    frame["service"] = frame["service"].astype(str)
    frame["host"] = frame["host"].astype(str)
    frame["summary"] = frame["summary"].astype(str)
    frame["labels_json"] = frame["labels_json"].astype(str)
    frame["ts"] = pd.to_datetime(frame["ts"])
    frame["resolved_ts"] = pd.to_datetime(frame["resolved_ts"])

    labels_df = pd.DataFrame(
        [
            {"alert_id": row.alert_id, "incident_id": json.loads(row.labels_json).get("incident_id", "noise")}
            for row in frame.itertuples(index=False)
        ]
    )

    alerts_path = root / "alerts.parquet"
    topology_path = root / "topology.csv"
    maintenance_path = root / "maintenance_windows.csv"
    labels_path = root / "incident_labels.csv"

    frame.to_parquet(alerts_path, index=False)
    topology.to_csv(topology_path, index=False)
    maintenance.to_csv(maintenance_path, index=False)
    labels_df.to_csv(labels_path, index=False)

    return frame, topology, maintenance, labels_df


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic alert data")
    parser.add_argument("--output-dir", default="data/generated")
    parser.add_argument("--scale", choices=["demo", "full"], default="demo")
    args = parser.parse_args()
    generate_alert_dataset(output_dir=args.output_dir, scale=args.scale)


if __name__ == "__main__":
    main()
