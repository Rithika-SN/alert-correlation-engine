from __future__ import annotations

from typing import Any


def generate_local_summary(signals: dict[str, Any]) -> str:
    rule_ids = ", ".join(signals.get("rule_ids", [])) if signals.get("rule_ids") else "noisy rules"
    services = ", ".join(signals.get("services", [])) if signals.get("services") else "unmapped service"
    severity = signals.get("severity_max", "unknown")
    summary = (
        f"Incident summary: {signals.get('cluster_size', 1)} alerts affected {services}. "
        f"The dominant rule set was {rule_ids}. Highest severity observed was {severity}. "
        f"This cluster spans {signals.get('time_span_minutes', 0)} minutes and includes {signals.get('suppression_count', 0)} suppressed events."
    )
    return summary
