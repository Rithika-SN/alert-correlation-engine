from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from pathlib import Path

import pandas as pd
import streamlit as st

from src.actionability.actionability_model import ActionabilityModel
from src.correlation.correlator import correlate_alerts
from src.db.duckdb_store import top_noisy_rules
from src.ingestion.dataset_loader import ensure_dataset
from src.suppression.suppression_store import read_suppressions, reverse_suppression


# ============================================================
# CONFIG
# ============================================================

ROOT = Path(__file__).resolve().parents[1]

st.set_page_config(
    page_title="Alert Correlation Engine",
    page_icon="🚨",
    layout="wide",
)


# ============================================================
# DATA
# ============================================================

@st.cache_data(ttl=30)
def load_runtime_data():

    dataset = ensure_dataset(str(ROOT))

    alerts = dataset["alerts"]
    topology = dataset["topology"]

    suppressions = read_suppressions()

    clusters = correlate_alerts(
        alerts,
        topology,
    )

    model = ActionabilityModel().fit(
        alerts,
        clusters,
    )

    source = dataset.get(
        "source",
        "generated",
    )

    return (
        alerts,
        topology,
        suppressions,
        clusters,
        model,
        source,
    )


# ============================================================
# HEADER
# ============================================================

def render_header(alerts, clusters, source):

    st.title("🚨 Alert Correlation Engine")

    st.caption(
        "From thousands of alerts to actionable incidents — automatically."
    )

    col1, col2, col3 = st.columns(3)

    with col1:
        st.info(
            f"📡 Data source: {source}"
        )

    with col2:
        st.info(
            f"🔔 {len(alerts):,} alerts processed"
        )

    with col3:
        st.info(
            f"🧩 {len(clusters):,} incident clusters"
        )


# ============================================================
# OVERVIEW
# ============================================================

def overview_tab(alerts, suppressions, clusters):

    suppression_pct = (
        len(suppressions) / len(alerts) * 100
        if len(alerts)
        else 0
    )

    unique_services = (
        alerts["service"].nunique()
        if "service" in alerts.columns
        else 0
    )

    unique_hosts = (
        alerts["host"].nunique()
        if "host" in alerts.columns
        else 0
    )

    st.header("System Overview")

    # --------------------------------------------------------
    # KPI CARDS
    # --------------------------------------------------------

    c1, c2, c3, c4 = st.columns(4)

    with c1:
        st.metric(
            "Total Alerts",
            f"{len(alerts):,}",
        )

    with c2:
        st.metric(
            "Incident Clusters",
            f"{len(clusters):,}",
        )

    with c3:
        st.metric(
            "Suppressed",
            f"{suppression_pct:.2f}%",
        )

    with c4:
        st.metric(
            "Services",
            f"{unique_services:,}",
        )

    st.divider()

    # --------------------------------------------------------
    # ALERT VOLUME
    # --------------------------------------------------------

    left, right = st.columns(2)

    with left:

        st.subheader("📈 Alert Volume")

        volume = alerts.copy()

        volume["ts"] = pd.to_datetime(
            volume["ts"],
            errors="coerce",
        )

        volume = (
            volume
            .dropna(subset=["ts"])
            .set_index("ts")
            .resample("H")
            .size()
        )

        if not volume.empty:
            st.line_chart(
                volume,
                use_container_width=True,
            )
        else:
            st.info(
                "No alert volume data available."
            )

    # --------------------------------------------------------
    # SEVERITY
    # --------------------------------------------------------

    with right:

        st.subheader("🎯 Severity Distribution")

        if "severity" in alerts.columns:

            severity = (
                alerts["severity"]
                .value_counts()
            )

            st.bar_chart(
                severity,
                use_container_width=True,
            )

        else:
            st.info(
                "Severity information unavailable."
            )

    st.divider()

    # --------------------------------------------------------
    # NOISIEST RULES
    # --------------------------------------------------------

    st.subheader("🔥 Noisiest Alert Rules")

    noisy_rules = top_noisy_rules(
        limit=10
    )

    if not noisy_rules.empty:

        st.bar_chart(
            noisy_rules.set_index("rule_id"),
            use_container_width=True,
        )

        st.dataframe(
            noisy_rules,
            use_container_width=True,
            hide_index=True,
        )

    st.divider()

    # --------------------------------------------------------
    # SYSTEM FOOTPRINT
    # --------------------------------------------------------

    st.subheader("🖥️ System Footprint")

    c1, c2, c3 = st.columns(3)

    with c1:
        st.metric(
            "Unique Hosts",
            f"{unique_hosts:,}",
        )

    with c2:

        rules = (
            alerts["rule_id"].nunique()
            if "rule_id" in alerts.columns
            else 0
        )

        st.metric(
            "Alert Rules",
            f"{rules:,}",
        )

    with c3:
        st.metric(
            "Suppression Records",
            f"{len(suppressions):,}",
        )


# ============================================================
# INCIDENTS
# ============================================================

def incident_tab(alerts, clusters, model):

    st.header("🧩 Incident Intelligence")

    if not clusters:

        st.info(
            "No incident clusters detected."
        )

        return

    # --------------------------------------------------------
    # CLUSTER TABLE
    # --------------------------------------------------------

    cluster_rows = []

    for cluster in clusters:

        start = cluster["start_ts"]
        end = cluster["end_ts"]

        duration = (
            end - start
        ).total_seconds() / 60

        cluster_rows.append(
            {
                "Cluster ID": cluster["id"],
                "Alerts": cluster["size"],
                "Services": ", ".join(
                    cluster["services"]
                ),
                "Duration (min)": round(
                    duration,
                    2,
                ),
            }
        )

    cluster_df = pd.DataFrame(
        cluster_rows
    )

    # --------------------------------------------------------
    # METRICS
    # --------------------------------------------------------

    c1, c2, c3 = st.columns(3)

    with c1:
        st.metric(
            "Detected Incidents",
            len(clusters),
        )

    with c2:

        average_size = (
            sum(
                c["size"]
                for c in clusters
            )
            / len(clusters)
        )

        st.metric(
            "Average Alerts / Incident",
            f"{average_size:.1f}",
        )

    with c3:

        largest = max(
            clusters,
            key=lambda x: x["size"],
        )

        st.metric(
            "Largest Incident",
            largest["size"],
        )

    st.dataframe(
        cluster_df,
        use_container_width=True,
        hide_index=True,
    )

    # --------------------------------------------------------
    # SELECT INCIDENT
    # --------------------------------------------------------

    st.subheader("🔎 Investigate Incident")

    selected_id = st.selectbox(
        "Select incident cluster",
        [
            cluster["id"]
            for cluster in clusters
        ],
    )

    cluster = next(
        item
        for item in clusters
        if item["id"] == selected_id
    )

    st.divider()

    # --------------------------------------------------------
    # INCIDENT DETAILS
    # --------------------------------------------------------

    left, right = st.columns(2)

    with left:

        st.subheader(
            "Incident Details"
        )

        st.write(
            f"**Incident ID:** `{cluster['id']}`"
        )

        st.write(
            f"**Alerts:** {cluster['size']}"
        )

        st.write(
            "**Services:** "
            + ", ".join(
                cluster["services"]
            )
        )

        duration = (
            cluster["end_ts"]
            - cluster["start_ts"]
        ).total_seconds() / 60

        st.write(
            f"**Duration:** {duration:.2f} minutes"
        )

    # --------------------------------------------------------
    # AI SUMMARY
    # --------------------------------------------------------

    with right:

        st.subheader(
            "🤖 AI Incident Summary"
        )

        try:

            from src.agent.summary_agent_graph import (
                summarize_cluster,
            )

            summary = summarize_cluster(
                cluster
            )

            st.write(summary)

        except Exception as error:

            st.warning(
                f"Summary unavailable: {error}"
            )

    # --------------------------------------------------------
    # ALERT EVIDENCE
    # --------------------------------------------------------

    st.subheader(
        "📋 Alert Evidence"
    )

    raw_df = pd.DataFrame(
        cluster["alerts"]
    )

    columns = [
        "alert_id",
        "service",
        "host",
        "severity",
        "rule_id",
        "ts",
    ]

    available_columns = [
        column
        for column in columns
        if column in raw_df.columns
    ]

    if available_columns:

        st.dataframe(
            raw_df[available_columns],
            use_container_width=True,
            hide_index=True,
        )

    # --------------------------------------------------------
    # SERVICE TOPOLOGY
    # --------------------------------------------------------

    st.subheader(
        "🔗 Service Relationship"
    )

    services = cluster.get(
        "services",
        [],
    )

    if services:

        dot = "digraph incident {\n"
        dot += "rankdir=LR;\n"

        for service in services:

            dot += (
                f'"{service}" '
                '[shape=box];\n'
            )

        for i in range(
            len(services) - 1
        ):

            dot += (
                f'"{services[i]}" '
                f'-> "{services[i + 1]}";\n'
            )

        dot += "}"

        st.graphviz_chart(
            dot,
            use_container_width=True,
        )


# ============================================================
# SUPPRESSION
# ============================================================

def suppression_tab(alerts, suppressions):

    st.header(
        "🛡️ Suppression & Noise Control"
    )

    if suppressions.empty:

        st.success(
            "No suppression records currently exist."
        )

        return

    displayed = suppressions.copy()

    displayed["reversed"] = (
        displayed["reversed"]
        .astype(bool)
    )

    active = displayed[
        ~displayed["reversed"]
    ]

    reversed_rows = displayed[
        displayed["reversed"]
    ]

    # --------------------------------------------------------
    # METRICS
    # --------------------------------------------------------

    c1, c2, c3 = st.columns(3)

    with c1:
        st.metric(
            "Total Records",
            len(displayed),
        )

    with c2:
        st.metric(
            "Active",
            len(active),
        )

    with c3:
        st.metric(
            "Reversed",
            len(reversed_rows),
        )

    st.divider()

    # --------------------------------------------------------
    # ACTIVE SUPPRESSIONS
    # --------------------------------------------------------

    st.subheader(
        "Active Suppressions"
    )

    for index, row in displayed.iterrows():

        if row["reversed"]:
            continue

        alert_id = str(
            row["alert_id"]
        )

        reason = str(
            row["reason"]
        )

        col1, col2, col3 = st.columns(
            [2, 5, 1]
        )

        with col1:
            st.write(
                f"**{alert_id}**"
            )

        with col2:
            st.caption(
                f"Reason: {reason}"
            )

        with col3:

            # Unique key fixes the
            # Streamlit duplicate-key error.

            button_key = (
                f"reverse_{alert_id}_{index}"
            )

            if st.button(
                "Reverse",
                key=button_key,
            ):

                success = reverse_suppression(
                    alert_id
                )

                if success:

                    st.success(
                        f"Reversed {alert_id}"
                    )

                    st.cache_data.clear()

                    st.rerun()

                else:

                    st.error(
                        "Unable to reverse suppression."
                    )

    st.divider()

    # --------------------------------------------------------
    # AUDIT TABLE
    # --------------------------------------------------------

    st.subheader(
        "Suppression Audit"
    )

    columns = [
        "alert_id",
        "reason",
        "suppressed_at",
        "reversible",
        "reversed",
    ]

    available = [
        column
        for column in columns
        if column in displayed.columns
    ]

    st.dataframe(
        displayed[available],
        use_container_width=True,
        hide_index=True,
    )

    # --------------------------------------------------------
    # REASONS
    # --------------------------------------------------------

    st.subheader(
        "Suppression Reasons"
    )

    if "reason" in displayed.columns:

        reason_counts = (
            displayed["reason"]
            .value_counts()
        )

        st.bar_chart(
            reason_counts,
            use_container_width=True,
        )


# ============================================================
# MODEL
# ============================================================

def model_tab(alerts, clusters, model):

    st.header(
        "🧠 AI & Methodology"
    )

    st.write(
        """
        The actionability layer evaluates correlated incidents
        using alert characteristics, rule behavior, cluster
        information and host context.
        """
    )

    # --------------------------------------------------------
    # FEATURE IMPORTANCE
    # --------------------------------------------------------

    if (
        hasattr(model, "model")
        and hasattr(
            model.model,
            "feature_importances_",
        )
    ):

        feature_names = [
            "severity_score",
            "rule_frequency",
            "cluster_size",
            "hour_of_day",
            "host_history",
        ]

        importances = (
            model.model
            .feature_importances_
        )

        importance_df = pd.DataFrame(
            {
                "Feature": feature_names,
                "Importance": importances,
            }
        )

        importance_df = (
            importance_df
            .sort_values(
                "Importance",
                ascending=False,
            )
            .set_index("Feature")
        )

        st.subheader(
            "Feature Importance"
        )

        st.bar_chart(
            importance_df,
            use_container_width=True,
        )

    # --------------------------------------------------------
    # PIPELINE
    # --------------------------------------------------------

    st.subheader(
        "⚙️ Correlation Pipeline"
    )

    steps = [
        "Alert ingestion",
        "Alert deduplication",
        "Time-based correlation",
        "Service topology correlation",
        "Text similarity",
        "Noise suppression",
        "Actionability scoring",
        "AI incident summary",
    ]

    for number, step in enumerate(
        steps,
        start=1,
    ):

        st.write(
            f"**{number}.** {step}"
        )

    st.subheader(
        "Current Configuration"
    )

    c1, c2, c3 = st.columns(3)

    with c1:
        st.metric(
            "Time Weight",
            "0.55",
        )

    with c2:
        st.metric(
            "Topology Weight",
            "0.45",
        )

    with c3:
        st.metric(
            "Text Similarity",
            "Enabled",
        )


# ============================================================
# SIDEBAR
# ============================================================

def render_sidebar():

    with st.sidebar:

        st.title("🚨 Alert Engine")

        st.caption("Incident intelligence console")

        st.divider()

        st.subheader("Data Source")

        data_source = st.radio(
            "Choose source",
            [
                "Synthetic dataset",
                "External dataset",
                "Upload CSV",
            ],
            index=0,
            key="data_source",
        )

        # ----------------------------------------------------
        # CSV UPLOAD
        # ----------------------------------------------------

        uploaded_file = None

        if data_source == "Upload CSV":

            uploaded_file = st.file_uploader(
                "Upload your alert CSV",
                type=["csv"],
                help="Upload a CSV containing alert records.",
            )

            if uploaded_file is not None:
                st.success(
                    f"✅ Uploaded: {uploaded_file.name}"
                )

                try:
                    preview = pd.read_csv(
                        uploaded_file
                    )

                    st.caption(
                        f"{len(preview):,} rows detected"
                    )

                    st.dataframe(
                        preview.head(5),
                        use_container_width=True,
                    )

                except Exception as e:
                    st.error(
                        f"Could not read CSV: {e}"
                    )

        st.divider()

        st.subheader("Correlation")

        st.slider(
            "Time correlation",
            0.0,
            1.0,
            0.55,
            0.05,
            key="w_time",
        )

        st.slider(
            "Topology correlation",
            0.0,
            1.0,
            0.45,
            0.05,
            key="w_topo",
        )

        st.divider()

        st.subheader("Display")

        st.checkbox(
            "Show alert details",
            value=True,
            key="show_alert_details",
        )

        st.checkbox(
            "Show service graph",
            value=True,
            key="show_service_graph",
        )


# ============================================================
# MAIN
# ============================================================

def main():

    render_sidebar()

    (
        alerts,
        topology,
        suppressions,
        clusters,
        model,
        source,
    ) = load_runtime_data()

    render_header(
        alerts,
        clusters,
        source,
    )

    tabs = st.tabs(
        [
            "📊 Overview",
            "🧩 Incidents",
            "🛡️ Suppression",
            "🧠 AI & Methodology",
        ]
    )

    with tabs[0]:

        overview_tab(
            alerts,
            suppressions,
            clusters,
        )

    with tabs[1]:

        incident_tab(
            alerts,
            clusters,
            model,
        )

    with tabs[2]:

        suppression_tab(
            alerts,
            suppressions,
        )

    with tabs[3]:

        model_tab(
            alerts,
            clusters,
            model,
        )


if __name__ == "__main__":
    main()
    