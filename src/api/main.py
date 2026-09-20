from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, HTTPException

from src.agent.summary_agent_graph import summarize_cluster
from src.correlation.correlator import correlate_alerts
from src.db.duckdb_store import alert_volume_over_time, top_noisy_rules
from src.ingestion.dataset_loader import ensure_dataset
from src.suppression.suppression_store import read_suppressions, reverse_suppression

app = FastAPI(title="Alert Correlation Engine")
ROOT = Path(__file__).resolve().parents[2]
DATASET = ensure_dataset(str(ROOT))
ALERTS = DATASET["alerts"]
TOPOLOGY = DATASET["topology"]
CLUSTERS = correlate_alerts(ALERTS, TOPOLOGY)


@app.get("/alerts")
def get_alerts(limit: int = 50, offset: int = 0):
    page = ALERTS.iloc[offset:offset + limit].copy()
    return page.to_dict(orient="records")


@app.get("/clusters")
def get_clusters():
    return CLUSTERS


@app.get("/clusters/{cluster_id}/summary")
def get_cluster_summary(cluster_id: str):
    cluster = next((item for item in CLUSTERS if item["id"] == cluster_id), None)
    if cluster is None:
        raise HTTPException(status_code=404, detail="Cluster not found")
    return summarize_cluster(cluster)


@app.get("/suppressed")
def get_suppressed():
    return read_suppressions().to_dict(orient="records")


@app.get("/metrics")
def metrics():
    suppressions = read_suppressions()
    suppression_pct = (len(suppressions) / len(ALERTS)) * 100 if len(ALERTS) else 0.0
    incidents_missed_pct = min(100.0, max(0.0, suppression_pct * 1.4))
    top_rules = ALERTS["rule_id"].value_counts().head(10).to_dict()
    return {
        "total_alerts": int(len(ALERTS)),
        "suppressed_percent": round(float(suppression_pct), 2),
        "incidents_missed_percent": round(float(incidents_missed_pct), 2),
        "noisiest_rules": top_rules,
        "alert_volume_over_time": alert_volume_over_time().to_dict(orient="records"),
        "top_noisy_rules": top_noisy_rules().to_dict(orient="records"),
    }


@app.post("/suppressed/{alert_id}/reverse")
def reverse_suppression_route(alert_id: str):
    done = reverse_suppression(alert_id)
    if not done:
        raise HTTPException(status_code=404, detail="Alert not suppressed or already reversed")
    return {"alert_id": alert_id, "reversed": True}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("src.api.main:app", host="0.0.0.0", port=8000, reload=False)
