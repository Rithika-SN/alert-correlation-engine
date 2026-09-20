from __future__ import annotations

import logging
from typing import Any

from langgraph.graph import END, StateGraph

from src.agent.llm_client import generate_local_summary
from src.suppression.suppression_store import read_suppressions

logger = logging.getLogger(__name__)


def collect_cluster_alerts(state: dict[str, Any]) -> dict[str, Any]:
    cluster = state.get("cluster", {})
    alerts = cluster.get("alerts", [])
    state["cluster_alerts"] = alerts
    logger.info("collect_cluster_alerts: %s alerts", len(alerts))
    return state


def extract_signals(state: dict[str, Any]) -> dict[str, Any]:
    alerts = state.get("cluster_alerts", [])
    if not alerts:
        state["rule_ids"] = []
        state["services"] = []
        state["severity_max"] = "low"
        state["time_span_minutes"] = 0
        state["suppression_count"] = 0
        return state

    services = sorted({str(item.get("service")) for item in alerts if item.get("service")})
    rule_ids = sorted({str(item.get("rule_id")) for item in alerts if item.get("rule_id")})
    severities = [str(item.get("severity", "low")).lower() for item in alerts]
    severity_order = ["low", "medium", "high", "critical"]
    state["rule_ids"] = rule_ids
    state["services"] = services
    state["severity_max"] = max(severities, key=lambda value: severity_order.index(value)) if severities else "low"
    if alerts and all(item.get("ts") is not None for item in alerts):
        state["time_span_minutes"] = int((max(item.get("ts") for item in alerts) - min(item.get("ts") for item in alerts)).total_seconds() / 60)
    else:
        state["time_span_minutes"] = 0
    suppressions = read_suppressions()
    suppressed_alerts = set(suppressions["alert_id"].tolist()) if not suppressions.empty else set()
    state["suppression_count"] = len([alert for alert in alerts if str(alert.get("alert_id")) in suppressed_alerts])
    logger.info("extract_signals: %s services, %s rules", len(services), len(rule_ids))
    return state


def draft_summary(state: dict[str, Any]) -> dict[str, Any]:
    signals = {
        "cluster_size": len(state.get("cluster_alerts", [])),
        "rule_ids": state.get("rule_ids", []),
        "services": state.get("services", []),
        "severity_max": state.get("severity_max", "low"),
        "time_span_minutes": state.get("time_span_minutes", 0),
        "suppression_count": state.get("suppression_count", 0),
    }
    state["draft_summary"] = generate_local_summary(signals)
    logger.info("draft_summary created")
    return state


def attach_audit_trail(state: dict[str, Any]) -> dict[str, Any]:
    suppressions = read_suppressions()
    alert_ids = {str(item.get("alert_id")) for item in state.get("cluster_alerts", [])}
    if suppressions.empty:
        state["audit_trail"] = []
        return state
    state["audit_trail"] = suppressions[suppressions["alert_id"].isin(list(alert_ids))].to_dict("records") if not suppressions.empty else []
    return state


def finalize_summary(state: dict[str, Any]) -> dict[str, Any]:
    cluster = state.get("cluster", {})
    title = f"Incident {cluster.get('id', 'unknown')}"
    final = {
        "title": title,
        "services": state.get("services", []),
        "summary": state.get("draft_summary", "No summary generated."),
        "audit_trail": state.get("audit_trail", []),
        "cluster_size": len(state.get("cluster_alerts", [])),
        "rule_ids": state.get("rule_ids", []),
    }
    state["final_summary"] = final
    logger.info("finalize_summary ready for %s", title)
    return state


def build_summary_graph():
    workflow = StateGraph(dict)
    workflow.add_node("collect_cluster_alerts", collect_cluster_alerts)
    workflow.add_node("extract_signals", extract_signals)
    workflow.add_node("draft_summary", draft_summary)
    workflow.add_node("attach_audit_trail", attach_audit_trail)
    workflow.add_node("finalize", finalize_summary)

    workflow.set_entry_point("collect_cluster_alerts")
    workflow.add_edge("collect_cluster_alerts", "extract_signals")
    workflow.add_edge("extract_signals", "draft_summary")
    workflow.add_edge("draft_summary", "attach_audit_trail")
    workflow.add_edge("attach_audit_trail", "finalize")
    workflow.add_edge("finalize", END)
    return workflow.compile()


SUMMARY_GRAPH = build_summary_graph()


def summarize_cluster(cluster: dict[str, Any]) -> dict[str, Any]:
    result = SUMMARY_GRAPH.invoke({"cluster": cluster})
    return result.get("final_summary", {"title": "incident", "summary": "No summary generated."})
