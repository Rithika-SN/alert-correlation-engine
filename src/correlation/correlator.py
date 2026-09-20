from __future__ import annotations

from collections import defaultdict

import pandas as pd

from src.correlation.topology_graph import build_topology_graph, topology_similarity

W_TIME = 0.55
W_TOPO = 0.45
DEFAULT_TIME_WINDOW_MINUTES = 5
DEFAULT_TOPOLOGY_HOPS = 3


def incident_score(time_delta_seconds: float, topology_similarity_score: float, time_window_seconds: float = 300.0, w_time: float = W_TIME, w_topo: float = W_TOPO) -> float:
    time_score = max(0.0, 1.0 - (time_delta_seconds / time_window_seconds))
    return w_time * time_score + w_topo * topology_similarity_score


def correlate_alerts(alerts: pd.DataFrame, topology: pd.DataFrame, time_window_minutes: int = DEFAULT_TIME_WINDOW_MINUTES, w_time: float = W_TIME, w_topo: float = W_TOPO) -> list[dict]:
    if alerts.empty:
        return []

    alerts = alerts.sort_values("ts").reset_index(drop=True).copy()
    alerts["ts"] = pd.to_datetime(alerts["ts"], errors="coerce")

    graph = build_topology_graph(topology)
    parent = {str(alert_id): str(alert_id) for alert_id in alerts["alert_id"].tolist()}

    def find(node: str) -> str:
        while parent[node] != node:
            parent[node] = parent[parent[node]]
            node = parent[node]
        return node

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    for i in range(len(alerts)):
        current = alerts.iloc[i]
        current_ts = current["ts"]
        if pd.isna(current_ts):
            continue
        window_end = current_ts + pd.Timedelta(minutes=time_window_minutes)
        for j in range(i + 1, len(alerts)):
            other = alerts.iloc[j]
            other_ts = other["ts"]
            if pd.isna(other_ts) or other_ts > window_end:
                break
            delta_seconds = max(0.0, (other_ts - current_ts).total_seconds())
            topo_score = topology_similarity(graph, str(current["service"]), str(other["service"]), max_hops=DEFAULT_TOPOLOGY_HOPS)
            score = incident_score(delta_seconds, topo_score, time_window_seconds=time_window_minutes * 60, w_time=w_time, w_topo=w_topo)
            same_service = str(current["service"]) == str(other["service"])
            if score >= 0.62 or (same_service and delta_seconds <= 60):
                union(str(current["alert_id"]), str(other["alert_id"]))

    buckets: dict[str, list[object]] = defaultdict(list)
    for row in alerts.itertuples(index=False):
        buckets[find(str(row.alert_id))].append(row)

    clusters = []
    for group in buckets.values():
        cluster_alerts = [row._asdict() for row in group]
        cluster_alerts = sorted(cluster_alerts, key=lambda item: item["ts"])
        services = sorted({str(item["service"]) for item in cluster_alerts})
        clusters.append(
            {
                "id": f"cluster_{len(clusters) + 1:04d}",
                "alerts": cluster_alerts,
                "services": services,
                "start_ts": cluster_alerts[0]["ts"],
                "end_ts": cluster_alerts[-1]["ts"],
                "size": len(cluster_alerts),
            }
        )
    return sorted(clusters, key=lambda item: item["size"], reverse=True)
