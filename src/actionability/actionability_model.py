from __future__ import annotations

from typing import Any

import pandas as pd
from sklearn.ensemble import RandomForestClassifier


class ActionabilityModel:
    def __init__(self):
        self.model = RandomForestClassifier(n_estimators=80, random_state=42)
        self.feature_names = ["severity_score", "rule_frequency", "cluster_size", "hour_of_day", "host_history"]
        self.is_fitted = False

    def _as_features(self, record: dict[str, Any]) -> dict[str, float]:
        severity_map = {"low": 1, "medium": 2, "high": 3, "critical": 4}
        ts = pd.to_datetime(record.get("ts"), errors="coerce")
        return {
            "severity_score": float(severity_map.get(str(record.get("severity", "low")).lower(), 1)),
            "rule_frequency": float(record.get("rule_frequency", 1)),
            "cluster_size": float(record.get("cluster_size", 1)),
            "hour_of_day": float(ts.hour if pd.notna(ts) else 0),
            "host_history": float(record.get("host_history", 1)),
        }

    def fit(self, alerts: pd.DataFrame, clusters: list[dict] | None = None):
        rows = []
        if alerts.empty:
            self.is_fitted = True
            return self

        for row in alerts.to_dict("records"):
            cluster_size = 1
            if clusters:
                for cluster in clusters:
                    cluster_ids = {alert.get("alert_id") for alert in cluster.get("alerts", [])}
                    if row.get("alert_id") in cluster_ids:
                        cluster_size = cluster.get("size", 1)
                        break
            rule_frequency = 1.0
            if isinstance(row.get("rule_id"), str):
                rule_frequency = 1.0 + max(0.0, row["rule_id"].count("rule_"))
            host_history = max(1.0, float(row.get("host_history", 1)))
            row_features = self._as_features({**row, "cluster_size": cluster_size, "rule_frequency": rule_frequency, "host_history": host_history})
            rows.append(row_features)

        labels = []
        for item in rows:
            label = 1 if (item["severity_score"] >= 3 or item["cluster_size"] >= 2 or item["hour_of_day"] in (0, 1, 2, 3, 4, 5)) else 0
            labels.append(label)

        X = pd.DataFrame(rows)[self.feature_names]
        self.model.fit(X, labels)
        self.is_fitted = True
        return self

    def predict_actionability(self, alert_or_cluster):
        if not self.is_fitted:
            return 0.5

        if isinstance(alert_or_cluster, dict) and "alerts" in alert_or_cluster:
            records = alert_or_cluster["alerts"]
            feature_rows = []
            host_set = {str(item.get("host")) for item in records if item.get("host")}
            for record in records:
                feature_rows.append(
                    self._as_features(
                        {
                            **record,
                            "cluster_size": len(records),
                            "rule_frequency": 1.0 + max(0.0, len({a.get("rule_id") for a in records if a.get("rule_id")}) / 10.0),
                            "host_history": len(host_set),
                        }
                    )
                )
            X = pd.DataFrame(feature_rows)[self.feature_names]
            if X.empty:
                return 0.5
            return float(self.model.predict_proba(X)[:, 1].mean())

        record = alert_or_cluster
        features = self._as_features({**record, "cluster_size": 1, "rule_frequency": 1.0, "host_history": 1.0})
        X = pd.DataFrame([features])[self.feature_names]
        return float(self.model.predict_proba(X)[0, 1])


def build_actionability_model(alerts: pd.DataFrame, clusters: list[dict] | None = None) -> ActionabilityModel:
    model = ActionabilityModel()
    return model.fit(alerts, clusters)


def predict_actionability(alert_or_cluster, model: ActionabilityModel | None = None) -> float:
    if model is None:
        return 0.5
    return model.predict_actionability(alert_or_cluster)
