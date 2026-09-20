from __future__ import annotations

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity


def merge_residual_clusters(alerts: pd.DataFrame, cluster_items: list[dict], threshold: float = 0.75) -> list[dict]:
    if alerts.empty:
        return cluster_items

    remaining = alerts.copy()
    if not cluster_items:
        return [{"id": "cluster_0001", "alerts": remaining.to_dict("records"), "services": sorted(remaining["service"].unique()), "size": len(remaining)}]

    vectors = TfidfVectorizer(stop_words="english")
    docs = [str(item) for item in remaining["summary"].tolist()]
    if not docs:
        return cluster_items

    matrix = vectors.fit_transform(docs)
    merged = [dict(cluster) for cluster in cluster_items]

    for i in range(len(remaining)):
        best_cluster = None
        best_score = 0.0
        for idx, cluster in enumerate(merged):
            if not cluster.get("alerts"):
                continue
            cluster_text = " ".join(str(alert.get("summary", "")) for alert in cluster["alerts"])
            if not cluster_text:
                continue
            cluster_vector = vectors.transform([cluster_text])[0]
            score = cosine_similarity(cluster_vector.reshape(1, -1), matrix[i].reshape(1, -1))[0][0]
            if score > best_score:
                best_score = float(score)
                best_cluster = idx
        if best_cluster is not None and best_score >= threshold:
            merged[best_cluster]["alerts"].append(remaining.iloc[i].to_dict())
            merged[best_cluster]["size"] = len(merged[best_cluster]["alerts"])
            merged[best_cluster]["services"] = sorted({str(row["service"]) for row in merged[best_cluster]["alerts"]})
        else:
            merged.append(
                {
                    "id": f"cluster_{len(merged) + 1:04d}",
                    "alerts": [remaining.iloc[i].to_dict()],
                    "services": [str(remaining.iloc[i]["service"])],
                    "size": 1,
                }
            )
    return merged
