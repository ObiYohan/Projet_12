"""Tableau de bord KPI de l'ETL - lecture des métriques enregistrées par pipeline.metrics.

Lancer avec :
    uv run streamlit run streamlit_dashboard.py

"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

import pandas as pd
import plotly.express as px
import streamlit as st

from pipeline.metrics import DEFAULT_METRICS_PATH, load_metrics

# Palette
COLOR_BLUE = "#2a78d6"
COLOR_ORANGE = "#eb6834"
COLOR_AQUA = "#1baf7a"
COLOR_VIOLET = "#4a3aa7"
STATUS_GOOD = "#0ca30c"
STATUS_WARNING = "#fab219"
STATUS_CRITICAL = "#d03b3b"

PRECISION_GOOD_THRESHOLD = 80.0
PRECISION_WARNING_THRESHOLD = 50.0

st.set_page_config(page_title="Suivi de la chaîne de données", page_icon="📊", layout="wide")

CHART_SURFACE = "#fcfcfb"
INK_PRIMARY = "#0b0b0b"
INK_MUTED = "#898781"
GRIDLINE = "#e1e0d9"
AXIS_LINE = "#c3c2b7"


def apply_light_theme(fig):
    """Force a light chart surface with dark, readable text.

    Without this, Plotly picks its text color from the browser/OS theme (dark
    mode -> white text) while plot_bgcolor/paper_bgcolor stay hardcoded light,
    producing invisible white-on-white text and labels.
    """
    fig.update_layout(
        plot_bgcolor=CHART_SURFACE,
        paper_bgcolor=CHART_SURFACE,
        font_color=INK_PRIMARY,
        legend_font_color=INK_PRIMARY,
        title_font_color=INK_PRIMARY,
    )
    fig.update_xaxes(color=INK_PRIMARY, gridcolor=GRIDLINE, linecolor=AXIS_LINE, title_font_color=INK_MUTED)
    fig.update_yaxes(color=INK_PRIMARY, gridcolor=GRIDLINE, linecolor=AXIS_LINE, title_font_color=INK_MUTED)
    fig.update_traces(textfont_color=INK_PRIMARY, selector=dict(type="bar"))
    return fig


def health_badge(pct_valid: float | None) -> str:
    """Traffic-light label for a source's data quality - never color alone (icon + text)."""
    if pct_valid is None:
        return "⚪ Pas encore de donnée"
    if pct_valid >= PRECISION_GOOD_THRESHOLD:
        return "🟢 Bon"
    if pct_valid >= PRECISION_WARNING_THRESHOLD:
        return "🟠 À surveiller"
    return "🔴 Problème"


st.title("📊 Suivi de la chaîne de données")
st.caption(
    "Ce tableau de bord répond à trois questions simples pour chaque source de données : "
    "**est-ce que les données récupérées sont bonnes ?** (qualité), "
    "**est-ce que ça va vite ?** (vitesse), et "
    "**est-ce que ça consomme beaucoup de ressources ?** (coût)."
)

metrics_path = PROJECT_ROOT / DEFAULT_METRICS_PATH
runs = load_metrics(metrics_path)

if runs.empty:
    st.warning(
        "Aucune exécution n'a encore été enregistrée. Lancez le pipeline "
        "(`uv run python -m pipeline.run --source ...`) ou le DAG Airflow pour "
        "commencer à alimenter ce tableau de bord."
    )
    st.stop()

sources = sorted(runs["source"].unique())
selected_sources = st.sidebar.multiselect("Sources à afficher", sources, default=sources)
runs = runs[runs["source"].isin(selected_sources)]

if runs.empty:
    st.info("Sélectionnez au moins une source dans le panneau de gauche.")
    st.stop()

latest = runs.sort_values("run_timestamp").groupby("source").tail(1).set_index("source")

# --- Indicateurs principaux -------------------------------------------------
st.subheader("Vue d'ensemble (dernière exécution de chaque source)")

col1, col2, col3, col4 = st.columns(4)
col1.metric("✅ Qualité moyenne", f"{latest['pct_valid'].mean():.0f} %", help="Part des lignes récupérées qui sont exploitables au final.")
col2.metric("⚡ Temps moyen", f"{latest['duration_seconds'].mean():.2f} s", help="Durée d'une exécution complète, de la lecture à l'export.")
col3.metric("💾 Mémoire moyenne", f"{latest['peak_memory_mb'].mean():.0f} Mo", help="Pic de mémoire utilisée pendant l'exécution.")
col4.metric("🔁 Exécutions suivies", f"{len(runs)}", help="Nombre total d'exécutions enregistrées, toutes sources confondues.")

st.divider()

# --- État par source ----------------------------------------------------
st.subheader("État par source")
status_table = latest[["pct_valid", "duration_seconds", "peak_memory_mb"]].copy()
status_table.insert(0, "État", status_table["pct_valid"].apply(health_badge))
status_table.columns = ["État", "Qualité (%)", "Temps (s)", "Mémoire (Mo)"]
st.dataframe(status_table.style.format({"Qualité (%)": "{:.1f}", "Temps (s)": "{:.2f}", "Mémoire (Mo)": "{:.0f}"}), width="stretch")
st.caption(
    f"🟢 Bon : ≥ {PRECISION_GOOD_THRESHOLD:.0f}% de données valides · "
    f"🟠 À surveiller : entre {PRECISION_WARNING_THRESHOLD:.0f}% et {PRECISION_GOOD_THRESHOLD:.0f}% · "
    f"🔴 Problème : < {PRECISION_WARNING_THRESHOLD:.0f}%"
)

st.divider()

# --- Précision ---------------------------------------------------------
st.subheader("✅ Précision : quelle part des données est exploitable ?")
fig_precision = px.bar(
    latest.reset_index(),
    x="source",
    y="pct_valid",
    labels={"source": "Source", "pct_valid": "% de lignes valides"},
    color_discrete_sequence=[COLOR_BLUE],
    text_auto=".1f",
)
fig_precision.update_layout(yaxis_range=[0, 105], showlegend=False)
apply_light_theme(fig_precision)
st.plotly_chart(fig_precision, width="stretch", theme=None)

drop_reason_columns = {
    "rows_dropped_missing_text": "Texte manquant",
    "rows_dropped_duplicate": "Doublon",
    "rows_dropped_label": "Étiquette non reconnue",
}
drop_df = latest[list(drop_reason_columns)].rename(columns=drop_reason_columns).reset_index()
drop_long = drop_df.melt(id_vars="source", var_name="Cause", value_name="Lignes rejetées")
fig_drops = px.bar(
    drop_long,
    x="source",
    y="Lignes rejetées",
    color="Cause",
    labels={"source": "Source"},
    color_discrete_sequence=[COLOR_BLUE, COLOR_ORANGE, COLOR_AQUA],
    barmode="stack",
)
apply_light_theme(fig_drops)
st.plotly_chart(fig_drops, width="stretch", theme=None)

st.divider()

# --- Vitesse -------------------------------------------------------------
st.subheader("⚡ Vitesse : combien de temps prend une exécution ?")
fig_speed = px.bar(
    latest.reset_index().sort_values("duration_seconds"),
    x="duration_seconds",
    y="source",
    orientation="h",
    labels={"source": "Source", "duration_seconds": "Durée (secondes)"},
    color_discrete_sequence=[COLOR_AQUA],
    text_auto=".2f",
)
fig_speed.update_layout(showlegend=False)
apply_light_theme(fig_speed)
st.plotly_chart(fig_speed, width="stretch", theme=None)

st.divider()

# --- Coût ------------------------------------------------------------------
st.subheader("💾 Coût : combien de ressources sont utilisées ?")
fig_cost = px.bar(
    latest.reset_index().sort_values("peak_memory_mb"),
    x="peak_memory_mb",
    y="source",
    orientation="h",
    labels={"source": "Source", "peak_memory_mb": "Mémoire pic (Mo)"},
    color_discrete_sequence=[COLOR_VIOLET],
    text_auto=".0f",
)
fig_cost.update_layout(showlegend=False)
apply_light_theme(fig_cost)
st.plotly_chart(fig_cost, width="stretch", theme=None)

st.divider()

# --- Détail technique (repliable) ------------------------------------------
with st.expander("Voir le détail technique (toutes les exécutions)"):
    st.dataframe(runs.sort_values("run_timestamp", ascending=False), width="stretch")
    st.caption(
        f"Source des données : `{DEFAULT_METRICS_PATH}` — un enregistrement par exécution du "
        "pipeline de transformation, ajouté automatiquement (voir `pipeline.metrics`)."
    )
