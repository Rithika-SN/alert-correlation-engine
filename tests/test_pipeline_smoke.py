from pathlib import Path

import pandas as pd

from src.generator.alert_stream_generator import generate_alert_dataset
from src.ingestion.dataset_loader import ensure_dataset
from src.dedup.flap_detector import detect_flapping_alerts, suppress_flapping_alerts
from src.correlation.correlator import correlate_alerts
from src.agent.summary_agent_graph import summarize_cluster


def test_pipeline_smoke(tmp_path):
    root = Path(__file__).resolve().parents[1]
    generated_dir = root / "data" / "generated"
    generated_dir.mkdir(parents=True, exist_ok=True)

    df, topology, maintenance, labels = generate_alert_dataset(output_dir=str(generated_dir), scale="demo")
    assert not df.empty
    assert {"alert_id", "ts", "service", "host", "severity", "rule_id", "summary"}.issubset(df.columns)

    ensure_dataset(root=str(root))
    alerts = pd.read_parquet(generated_dir / "alerts.parquet")
    flaps = detect_flapping_alerts(alerts)
    suppressed = suppress_flapping_alerts(alerts, flaps)
    assert len(suppressed) >= 0

    clusters = correlate_alerts(alerts, topology)
    assert len(clusters) >= 1

    sample_cluster = clusters[0]
    summary = summarize_cluster(sample_cluster)
    assert "incident" in summary["title"].lower() or "cluster" in summary["title"].lower()
    assert "services" in summary
