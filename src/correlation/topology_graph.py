from __future__ import annotations

import networkx as nx
import pandas as pd


def load_topology(path: str | None = None) -> pd.DataFrame:
    if path is None:
        return pd.DataFrame(columns=["from_service", "to_service", "call_rate"])
    return pd.read_csv(path)


def build_topology_graph(topology_df: pd.DataFrame) -> nx.DiGraph:
    graph = nx.DiGraph()
    if topology_df is None or topology_df.empty:
        return graph
    for row in topology_df.itertuples(index=False):
        if pd.isna(row.from_service) or pd.isna(row.to_service):
            continue
        graph.add_edge(str(row.from_service), str(row.to_service), weight=float(row.call_rate or 0.5))
    return graph


def walk_upstream_services(graph: nx.DiGraph, service: str, max_hops: int = 3):
    if not service or graph.number_of_nodes() == 0:
        return []
    queue = [(service, 0)]
    visited = {service}
    upstream = []
    while queue:
        current, depth = queue.pop(0)
        if depth >= max_hops:
            continue
        predecessors = list(graph.predecessors(current))
        for parent in predecessors:
            if parent not in visited:
                visited.add(parent)
                upstream.append(parent)
                queue.append((parent, depth + 1))
    return upstream


def topology_similarity(graph: nx.DiGraph, service_a: str, service_b: str, max_hops: int = 3) -> float:
    if not service_a or not service_b:
        return 0.0
    if service_a == service_b:
        return 1.0
    if graph.number_of_nodes() == 0:
        return 0.0

    if service_b in walk_upstream_services(graph, service_a, max_hops):
        return 0.85
    if service_a in walk_upstream_services(graph, service_b, max_hops):
        return 0.8

    if graph.has_edge(service_a, service_b):
        return float(graph[service_a][service_b].get("weight", 0.5))
    if graph.has_edge(service_b, service_a):
        return float(graph[service_b][service_a].get("weight", 0.5))

    try:
        path = nx.shortest_path(graph.to_undirected(), service_a, service_b, weight="weight")
        if len(path) > 1:
            return max(0.15, 1.0 / len(path))
    except Exception:
        pass
    return 0.0


def fail_open_cold_start_behavior(service: str) -> bool:
    """New services with no topology history are treated as singleton incidents until enough evidence is accumulated. This is fail-open, not fail-closed."""
    return True
