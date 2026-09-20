from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.generator.alert_stream_generator import generate_alert_dataset


def _external_dataset_exists(data_dir: Path) -> bool:
    external_dir = data_dir / "external"
    if not external_dir.exists():
        return False
    allowed = [
        external_dir / "alerts.csv",
        external_dir / "alerts.parquet",
        external_dir / "alerts.tsv",
        external_dir / "incident_alerts.csv",
    ]
    return any(path.exists() for path in allowed) or any(external_dir.glob("*.csv")) or any(external_dir.glob("*.parquet"))


def _load_external_dataset(data_dir: Path) -> dict:
    external_dir = data_dir / "external"
    alert_candidates = [
        external_dir / "alerts.parquet",
        external_dir / "alerts.csv",
        external_dir / "alerts.tsv",
        *sorted(external_dir.glob("*.parquet")),
        *sorted(external_dir.glob("*.csv")),
    ]
    alert_path = next((path for path in alert_candidates if path.exists() and "alert" in path.name.lower()), None)
    if alert_path is None:
        alert_path = external_dir / "alerts.csv"
    if alert_path.suffix.lower() == ".parquet":
        alert_df = pd.read_parquet(alert_path)
    else:
        alert_df = pd.read_csv(alert_path)

    topology = pd.read_csv(external_dir / "topology.csv") if (external_dir / "topology.csv").exists() else pd.DataFrame(columns=["from_service", "to_service", "call_rate"])
    maintenance = pd.read_csv(external_dir / "maintenance_windows.csv") if (external_dir / "maintenance_windows.csv").exists() else pd.DataFrame(columns=["service", "start", "end", "scope"])
    labels = pd.read_csv(external_dir / "incident_labels.csv") if (external_dir / "incident_labels.csv").exists() else pd.DataFrame(columns=["alert_id", "incident_id"])
    return {"alerts": alert_df, "topology": topology, "maintenance": maintenance, "labels": labels, "source": "external"}


def ensure_dataset(root: str = ".", uploaded_df: pd.DataFrame | None = None) -> dict:
    project_root = Path(root).resolve()
    data_dir = project_root / "data"
    generated_dir = data_dir / "generated"
    generated_dir.mkdir(parents=True, exist_ok=True)

    if uploaded_df is not None:
        return {"alerts": uploaded_df, "topology": pd.DataFrame(columns=["from_service", "to_service", "call_rate"]), "maintenance": pd.DataFrame(columns=["service", "start", "end", "scope"]), "labels": pd.DataFrame(columns=["alert_id", "incident_id"]), "source": "uploaded"}

    if _external_dataset_exists(data_dir):
        return _load_external_dataset(data_dir)

    alerts, topology, maintenance, labels = generate_alert_dataset(output_dir=str(generated_dir), scale="demo")
    return {"alerts": alerts, "topology": topology, "maintenance": maintenance, "labels": labels, "source": "generated"}


def load_alerts(root: str = ".") -> pd.DataFrame:
    dataset = ensure_dataset(root)
    return dataset["alerts"]


def load_topology(root: str = ".") -> pd.DataFrame:
    dataset = ensure_dataset(root)
    return dataset["topology"]


def load_maintenance(root: str = ".") -> pd.DataFrame:
    dataset = ensure_dataset(root)
    return dataset["maintenance"]
