"""Page 2: Eval Dashboard -- evaluation results visualization."""

from __future__ import annotations

import json
from pathlib import Path

import streamlit as st
import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
EVAL_DIR = PROJECT_ROOT / "data" / "eval"
TRACES_DIR = EVAL_DIR / "traces"

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Eval Dashboard | Play Attribution Intelligence",
    page_icon="📊",
    layout="wide",
)

st.title("Eval Dashboard")
st.markdown(
    "Automated evaluation scorecard for the attribution agent."
)

# ---------------------------------------------------------------------------
# Eval dimension definitions
# ---------------------------------------------------------------------------

# The eval framework supports two scoring schemas. The LLM-as-judge schema
# (5-point scale on clarity/actionability/completeness/tone/structure) and
# the quantitative schema (0-1 scale on attribution_accuracy,
# cause_identification, false_attribution, data_artifact_detection,
# narrative_quality, factual_grounding). We detect which one is present
# and render accordingly.

QUAL_DIMENSIONS = ["clarity", "actionability", "completeness", "tone", "structure"]
QUAL_DESCRIPTIONS = {
    "clarity": "Is the narrative easy to understand? Would a GM grasp the key message in 30 seconds?",
    "actionability": "Does it tell the business team what to do next?",
    "completeness": "Did it cover all relevant factors, or miss important ones?",
    "tone": "Is it appropriately analytical (not too hedged, not too confident)?",
    "structure": "Does it follow the expected report format?",
}

QUANT_DIMENSIONS = [
    "attribution_accuracy", "cause_identification", "false_attribution",
    "data_artifact_detection", "narrative_quality", "factual_grounding",
]
QUANT_WEIGHTS = {
    "attribution_accuracy": 0.30,
    "cause_identification": 0.25,
    "false_attribution": 0.15,
    "data_artifact_detection": 0.10,
    "narrative_quality": 0.10,
    "factual_grounding": 0.10,
}
QUANT_THRESHOLDS = {
    "attribution_accuracy": 0.70,
    "cause_identification": 0.80,
    "false_attribution": 0.90,
    "data_artifact_detection": 0.80,
    "narrative_quality": 0.70,
    "factual_grounding": 0.95,
}


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

@st.cache_data
def load_eval_results() -> list[dict] | None:
    """Load per-case eval results."""
    path = EVAL_DIR / "eval_results.json"
    if not path.exists():
        return None
    with open(path) as f:
        data = json.load(f)
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and "results" in data:
        return data["results"]
    return [data]


@st.cache_data
def load_eval_summary() -> dict | None:
    """Load aggregate eval summary."""
    path = EVAL_DIR / "eval_summary.json"
    if not path.exists():
        return None
    with open(path) as f:
        return json.load(f)


@st.cache_data
def load_trace_files() -> list[dict]:
    """Load all trace files from the traces directory."""
    traces = []
    if not TRACES_DIR.exists():
        return traces
    for f in sorted(TRACES_DIR.glob("*.json")):
        try:
            with open(f) as fh:
                traces.append(json.load(fh))
        except (json.JSONDecodeError, IOError):
            pass
    return traces


def _extract_score(value) -> float:
    """Extract a numeric score from a value that might be a dict or number."""
    if isinstance(value, dict):
        return float(value.get("score", 0))
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _detect_schema(data: list[dict] | dict) -> str:
    """Detect whether the eval data uses 'qualitative' (1-5) or 'quantitative' (0-1) schema."""
    sample = data[0] if isinstance(data, list) else data
    if not isinstance(sample, dict):
        return "unknown"

    # Check for qualitative dimensions
    if any(d in sample for d in QUAL_DIMENSIONS):
        return "qualitative"

    # Check for quantitative dimensions (possibly nested under 'scores')
    scores = sample.get("scores", sample)
    if any(d in scores for d in QUANT_DIMENSIONS):
        return "quantitative"

    # Check summary-level keys
    if "dimension_averages" in sample:
        return "quantitative"
    if "average_scores" in sample:
        avgs = sample["average_scores"]
        if any(d in avgs for d in QUANT_DIMENSIONS):
            return "quantitative"
        if any(d in avgs for d in QUAL_DIMENSIONS):
            return "qualitative"

    return "unknown"


# ---------------------------------------------------------------------------
# Main content
# ---------------------------------------------------------------------------

results = load_eval_results()
summary = load_eval_summary()

if results is None and summary is None:
    st.warning("No evaluation results found. Run the evaluation suite first.")

    st.code(
        "# Generate data first:\n"
        "python -m src.data_generator.generator\n\n"
        "# Then run evaluation:\n"
        "make eval\n"
        "# or: python -m src.eval",
        language="bash",
    )

    st.markdown("### What the eval measures")
    st.markdown("""
    The evaluation framework scores the agent across 6 dimensions:

    | Dimension | Weight | What it measures |
    |-----------|--------|-----------------|
    | Attribution Accuracy | 30% | How close are contribution percentages to ground truth? |
    | Cause Identification | 25% | Did the agent find all real causes? (F1 score) |
    | False Attribution | 15% | Did the agent hallucinate causes? |
    | Data Artifact Detection | 10% | Can it spot data quality issues vs real movements? |
    | Narrative Quality | 10% | Is the report clear, actionable, well-structured? |
    | Factual Grounding | 10% | Are all claims traceable to source data? |
    """)
    st.stop()

# Detect schema from available data
schema = "unknown"
if results:
    schema = _detect_schema(results)
if schema == "unknown" and summary:
    schema = _detect_schema(summary)

# ============================================================================
# QUANTITATIVE SCHEMA (0-1 scale, weighted dimensions)
# ============================================================================

if schema == "quantitative" or schema == "unknown":
    # ---- Aggregate Scorecard ----
    st.markdown("### Aggregate Scorecard")

    if summary:
        overall = summary.get("overall_weighted_score", 0)
        total_cases = summary.get("total_cases", 0)
        dim_avgs = summary.get("dimension_averages", {})

        cols = st.columns(4)
        cols[0].metric("Overall Score", f"{overall:.1%}")
        cols[1].metric("Total Cases", total_cases)

        if dim_avgs:
            best = max(dim_avgs, key=dim_avgs.get)
            worst = min(dim_avgs, key=dim_avgs.get)
            cols[2].metric("Best Dimension", best.replace("_", " ").title())
            cols[3].metric("Worst Dimension", worst.replace("_", " ").title())

    # ---- Dimension Score Bar Chart ----
    dim_avgs_to_plot = {}
    if summary and "dimension_averages" in summary:
        dim_avgs_to_plot = summary["dimension_averages"]
    elif results:
        # Compute from results
        for dim in QUANT_DIMENSIONS:
            vals = []
            for r in results:
                scores_obj = r.get("scores", r)
                if dim in scores_obj:
                    vals.append(_extract_score(scores_obj[dim]))
            if vals:
                dim_avgs_to_plot[dim] = sum(vals) / len(vals)

    if dim_avgs_to_plot:
        st.markdown("### Dimension Scores")

        import plotly.graph_objects as go

        dims = list(dim_avgs_to_plot.keys())
        scores_vals = [dim_avgs_to_plot[d] for d in dims]
        colors = [
            "#22c55e" if scores_vals[i] >= QUANT_THRESHOLDS.get(dims[i], 0.7) else "#ef4444"
            for i in range(len(dims))
        ]

        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=[d.replace("_", " ").title() for d in dims],
            y=scores_vals,
            marker_color=colors,
            text=[f"{s:.1%}" for s in scores_vals],
            textposition="outside",
        ))
        fig.update_layout(
            yaxis_range=[0, 1.1],
            yaxis_title="Score",
            height=400,
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig, use_container_width=True)

    # ---- Scores by Difficulty ----
    difficulty_data = summary.get("difficulty_breakdown", {}) if summary else {}
    if not difficulty_data and results:
        # Build from results
        has_difficulty = any("difficulty" in r for r in results)
        if has_difficulty:
            temp: dict[str, dict[str, list[float]]] = {}
            for r in results:
                diff = r.get("difficulty", "unknown")
                if diff not in temp:
                    temp[diff] = {}
                scores_obj = r.get("scores", r)
                for dim in QUANT_DIMENSIONS:
                    if dim in scores_obj:
                        temp[diff].setdefault(dim, []).append(_extract_score(scores_obj[dim]))
            difficulty_data = {
                diff: {dim: sum(v) / len(v) for dim, v in dims_dict.items()}
                for diff, dims_dict in temp.items()
            }

    if difficulty_data:
        st.markdown("---")
        st.markdown("### Scores by Difficulty")
        diff_df = pd.DataFrame(difficulty_data).T
        diff_df.index.name = "Difficulty"
        st.dataframe(
            diff_df.style.format("{:.1%}"),
            use_container_width=True,
        )

    # ---- Per-Case Detail Table ----
    if results:
        st.markdown("---")
        st.markdown("### Per-Case Results")

        case_rows = []
        for r in results:
            scores_obj = r.get("scores", r)
            row = {
                "Movement ID": r.get("movement_id", r.get("case_id", "")),
                "Metric": r.get("metric_name", r.get("metric", "")),
                "Market": r.get("market_id", r.get("market", "")),
                "Difficulty": r.get("difficulty", "N/A"),
                "Weighted Score": _extract_score(scores_obj.get("weighted_total", 0)),
            }
            for dim in QUANT_DIMENSIONS:
                if dim in scores_obj:
                    row[dim.replace("_", " ").title()] = _extract_score(scores_obj[dim])
            case_rows.append(row)

        case_df = pd.DataFrame(case_rows)

        # Filters
        filter_cols = st.columns(3)
        with filter_cols[0]:
            difficulties = sorted(case_df["Difficulty"].unique().tolist())
            sel_diff = st.multiselect("Filter by Difficulty", difficulties, default=difficulties)
        with filter_cols[1]:
            metrics = sorted(case_df["Metric"].dropna().unique().tolist()) if "Metric" in case_df.columns else []
            sel_metrics = st.multiselect("Filter by Metric", metrics, default=metrics) if metrics else metrics
        with filter_cols[2]:
            sort_by = st.selectbox(
                "Sort by",
                ["Weighted Score"] + [d.replace("_", " ").title() for d in QUANT_DIMENSIONS
                                      if d.replace("_", " ").title() in case_df.columns],
            )

        filtered_df = case_df[case_df["Difficulty"].isin(sel_diff)]
        if sel_metrics:
            filtered_df = filtered_df[filtered_df["Metric"].isin(sel_metrics)]

        sort_asc = st.checkbox("Ascending sort", value=True)
        filtered_df = filtered_df.sort_values(sort_by, ascending=sort_asc)

        fmt_dict = {col: "{:.1%}" for col in filtered_df.columns if col not in ("Movement ID", "Metric", "Market", "Difficulty")}
        st.dataframe(
            filtered_df.style.format(fmt_dict),
            use_container_width=True,
            height=min(600, 35 * len(filtered_df) + 38),
        )
        st.caption(f"Showing {len(filtered_df)} of {len(results)} cases")

    # ---- Failure Mode Analysis ----
    if results:
        st.markdown("---")
        st.markdown("### Failure Mode Analysis")
        st.markdown("Cases with lowest weighted scores where the agent struggles.")

        # Bottom N cases
        n_worst = min(10, len(case_rows))
        worst_df = pd.DataFrame(case_rows).nsmallest(n_worst, "Weighted Score")
        st.markdown(f"**Bottom {n_worst} cases** (lowest weighted score):")
        for _, row in worst_df.iterrows():
            ws = row.get("Weighted Score", 0)
            st.markdown(
                f"- **{row.get('Movement ID', 'N/A')}**: "
                f"{row.get('Metric', '')} in {row.get('Market', '')} "
                f"({row.get('Difficulty', '')}) -- Score: {ws:.1%}"
            )

        # Dimension with most failures (below threshold)
        dim_failures: dict[str, int] = {}
        for r in results:
            scores_obj = r.get("scores", r)
            for dim in QUANT_DIMENSIONS:
                if dim in scores_obj:
                    if _extract_score(scores_obj[dim]) < QUANT_THRESHOLDS.get(dim, 0.7):
                        dim_failures[dim] = dim_failures.get(dim, 0) + 1

        if dim_failures:
            st.markdown("**Dimensions with most below-threshold scores:**")
            for dim, count in sorted(dim_failures.items(), key=lambda x: -x[1]):
                threshold = QUANT_THRESHOLDS.get(dim, 0.7)
                st.markdown(
                    f"- **{dim.replace('_', ' ').title()}**: "
                    f"{count} cases below {threshold:.0%} threshold"
                )

# ============================================================================
# QUALITATIVE SCHEMA (1-5 scale, LLM-as-judge)
# ============================================================================

elif schema == "qualitative":
    # ---- Aggregate Scorecard ----
    st.markdown("### Aggregate Scorecard")

    # Compute average scores
    avg_scores: dict[str, float] = {}

    if summary:
        for dim in QUAL_DIMENSIONS:
            if dim in summary:
                avg_scores[dim] = _extract_score(summary[dim])
            elif "average_scores" in summary and dim in summary["average_scores"]:
                avg_scores[dim] = _extract_score(summary["average_scores"][dim])

    if not avg_scores and results:
        for dim in QUAL_DIMENSIONS:
            vals = [_extract_score(r[dim]) for r in results if dim in r]
            if vals:
                avg_scores[dim] = sum(vals) / len(vals)

    if avg_scores:
        from src.ui.components.metric_card import metric_card_row

        cards = []
        for dim in QUAL_DIMENSIONS:
            score = avg_scores.get(dim, 0)
            cards.append({
                "label": dim.title(),
                "value": score,
                "fmt": ".2f",
                "threshold": 3.0,
                "suffix": " / 5",
                "help_text": QUAL_DESCRIPTIONS.get(dim, ""),
            })
        metric_card_row(cards, columns=len(cards))

        overall = sum(avg_scores.values()) / max(len(avg_scores), 1)
        st.markdown(f"**Overall Average:** {overall:.2f} / 5.0")

    # ---- Radar Chart ----
    st.markdown("---")
    st.markdown("### Score Profile")

    if avg_scores:
        from src.ui.components.charts import score_radar_chart

        radar_data = {dim.title(): avg_scores[dim] for dim in QUAL_DIMENSIONS if dim in avg_scores}
        fig = score_radar_chart(radar_data)
        st.plotly_chart(fig, use_container_width=True)

    # ---- Score Distribution by Difficulty ----
    if results:
        has_difficulty = any("difficulty" in r for r in results)
        if has_difficulty:
            st.markdown("---")
            st.markdown("### Scores by Difficulty Level")

            from src.ui.components.charts import difficulty_bar_chart

            chart_data = []
            for r in results:
                row = {"difficulty": r.get("difficulty", "unknown")}
                for dim in QUAL_DIMENSIONS:
                    if dim in r:
                        row[dim] = _extract_score(r[dim])
                chart_data.append(row)

            fig = difficulty_bar_chart(chart_data)
            st.plotly_chart(fig, use_container_width=True)

    # ---- Per-Case Detail Table ----
    if results:
        st.markdown("---")
        st.markdown("### Per-Case Results")

        table_rows = []
        for i, r in enumerate(results):
            row: dict = {
                "Case": str(r.get("case_id", r.get("query", f"Case {i + 1}")))[:60],
                "Difficulty": r.get("difficulty", "N/A"),
            }
            for dim in QUAL_DIMENSIONS:
                if dim in r:
                    row[dim.title()] = _extract_score(r[dim])
            scores = [row.get(d.title(), 0) for d in QUAL_DIMENSIONS if d.title() in row]
            row["Avg"] = sum(scores) / max(len(scores), 1)
            table_rows.append(row)

        df = pd.DataFrame(table_rows)

        filter_col1, filter_col2 = st.columns(2)
        with filter_col1:
            difficulties = sorted(df["Difficulty"].unique().tolist())
            sel_difficulty = st.multiselect("Filter by Difficulty", difficulties, default=difficulties)
        with filter_col2:
            min_avg = st.slider("Minimum Average Score", 0.0, 5.0, 0.0, 0.5)

        filtered = df[df["Difficulty"].isin(sel_difficulty) & (df["Avg"] >= min_avg)]

        sort_col = st.selectbox(
            "Sort by",
            ["Avg"] + [d.title() for d in QUAL_DIMENSIONS if d.title() in df.columns],
        )
        sort_asc = st.checkbox("Ascending", value=False)
        filtered = filtered.sort_values(sort_col, ascending=sort_asc)

        st.dataframe(
            filtered,
            use_container_width=True,
            height=min(600, 35 * len(filtered) + 38),
            column_config={
                "Avg": st.column_config.NumberColumn(format="%.2f"),
                **{
                    d.title(): st.column_config.NumberColumn(format="%.1f")
                    for d in QUAL_DIMENSIONS
                },
            },
        )
        st.caption(f"Showing {len(filtered)} of {len(results)} cases")

    # ---- Failure Mode Analysis ----
    if results:
        st.markdown("---")
        st.markdown("### Failure Mode Analysis")
        st.markdown("Cases where the agent scored below **3.0** on any dimension.")

        failures = []
        for r in results:
            case_id = r.get("case_id", r.get("query", "unknown"))
            for dim in QUAL_DIMENSIONS:
                if dim in r:
                    score = _extract_score(r[dim])
                    if score < 3.0:
                        reason = ""
                        if isinstance(r[dim], dict):
                            reason = r[dim].get("reason", "")
                        failures.append({
                            "Case": str(case_id)[:60],
                            "Dimension": dim.title(),
                            "Score": score,
                            "Difficulty": r.get("difficulty", "N/A"),
                            "Reason": reason[:200],
                        })

        if failures:
            fail_df = pd.DataFrame(failures)
            dim_counts = fail_df["Dimension"].value_counts().reset_index()
            dim_counts.columns = ["Dimension", "Failure Count"]
            st.markdown("**Failures by dimension:**")
            st.dataframe(dim_counts, use_container_width=True, hide_index=True)

            st.markdown("**Individual failures:**")
            st.dataframe(fail_df, use_container_width=True, hide_index=True)
        else:
            st.success("No failures detected -- all scores are 3.0 or above.")

# ---- Raw JSON Viewer ----
st.markdown("---")
with st.expander("Raw Eval Data"):
    tab1, tab2 = st.tabs(["Results", "Summary"])
    with tab1:
        if results:
            st.json(results)
        else:
            st.info("No results data.")
    with tab2:
        if summary:
            st.json(summary)
        else:
            st.info("No summary data.")
