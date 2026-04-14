"""Page 1: Attribution Agent -- interactive natural language attribution queries."""

from __future__ import annotations

import asyncio
import json
import sys
import time
import traceback
from pathlib import Path

import streamlit as st
import pandas as pd
import yaml

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Load .env so ANTHROPIC_API_KEY is available (local dev path)
from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env", override=True)

# Streamlit Cloud path: promote st.secrets -> os.environ so the Anthropic
# client picks up the key regardless of which page is loaded first.
import os
try:
    for _key in ("ANTHROPIC_API_KEY",):
        if _key not in os.environ and _key in st.secrets:
            os.environ[_key] = st.secrets[_key]
except (FileNotFoundError, KeyError):
    pass

DATA_DIR = PROJECT_ROOT / "data"
SYNTHETIC_DIR = DATA_DIR / "synthetic"
CONFIG_DIR = PROJECT_ROOT / "config"

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Attribution Agent | Play Attribution Intelligence",
    page_icon="🔍",
    layout="wide",
)

st.title("Attribution Agent")
st.markdown(
    "Ask a natural language question about metric movements and get an "
    "analyst-quality attribution report."
)

# ---------------------------------------------------------------------------
# Example queries
# ---------------------------------------------------------------------------

EXAMPLE_QUERIES = [
    "Why did offer redemption rate increase 22% in India in late October 2024?",
    "What caused Play Points burn rate to spike 45% in India in late October?",
    "Why did DAU drop 15% globally on November 8-9 2024?",
    "What drove offer driven revenue decline in US Casual Games in early November?",
]


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

@st.cache_data
def load_dimensions_config() -> dict:
    """Load the dimensions.yaml config."""
    path = CONFIG_DIR / "dimensions.yaml"
    if not path.exists():
        return {}
    with open(path) as f:
        return yaml.safe_load(f) or {}


@st.cache_data
def load_local_config() -> dict:
    """Load the local.yaml config."""
    path = CONFIG_DIR / "local.yaml"
    if not path.exists():
        return {}
    with open(path) as f:
        return yaml.safe_load(f) or {}


# ---------------------------------------------------------------------------
# Pipeline runner
# ---------------------------------------------------------------------------

def _run_async(coro):
    """Run an async coroutine safely, handling existing event loops."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        # Streamlit runs its own event loop — use a new thread
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as pool:
            future = pool.submit(asyncio.run, coro)
            return future.result()
    else:
        return asyncio.run(coro)


def run_pipeline(query: str) -> dict:
    """Run the attribution pipeline and return results.

    Returns a dict with keys: parsed_query, attribution, narrative,
    grounding, trace, data_tables. Falls back gracefully if pipeline
    components are not yet implemented.
    """
    result: dict = {
        "parsed_query": None,
        "attribution": None,
        "narrative": None,
        "grounding": None,
        "trace": None,
        "data_tables": None,
        "error": None,
    }

    start_time = time.time()

    try:
        # Try to import and run the full pipeline
        from src.agent.pipeline import AttributionPipeline

        config_path = str(CONFIG_DIR / "local.yaml")
        pipeline = AttributionPipeline(config_path)
        pipeline_result = _run_async(pipeline.process(query))
        result.update(pipeline_result)

    except ImportError as e:
        # Pipeline not implemented yet -- run individual stages that exist
        st.warning(
            f"Full `AttributionPipeline` import failed: {e}. "
            "Running available stages individually."
        )
        result = _run_stages_individually(query)

    except Exception as e:
        result["error"] = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"

    elapsed_ms = (time.time() - start_time) * 1000

    # Build a trace if we don't have one
    if result.get("trace") is None:
        result["trace"] = {
            "trace_id": "ui-run",
            "query": query,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "total_duration_ms": elapsed_ms,
            "total_cost_usd": 0,
            "total_input_tokens": 0,
            "total_output_tokens": 0,
            "stages": [],
        }

    return result


def _run_stages_individually(query: str) -> dict:
    """Attempt to run pipeline stages that exist, skipping missing ones."""
    result: dict = {
        "parsed_query": None,
        "attribution": None,
        "narrative": None,
        "grounding": None,
        "trace": None,
        "data_tables": None,
        "error": None,
    }

    stages_trace: list[dict] = []
    dims_config = load_dimensions_config()
    local_config = load_local_config()

    # Stage 1: Query parsing
    try:
        from src.agent.query_parser import QueryParser
        from src.agent.llm_client import LLMClientFactory
        from src.observability.cost_tracker import CostTracker

        cost_tracker = CostTracker()
        llm_config = local_config.get("llm", {})
        llm = LLMClientFactory.create(llm_config, cost_tracker)

        parser = QueryParser(llm, dims_config)

        t0 = time.time()
        parsed = _run_async(parser.parse(query))
        parsed["original_query"] = query
        result["parsed_query"] = parsed
        stages_trace.append({
            "stage_name": "Query Parser",
            "duration_ms": (time.time() - t0) * 1000,
            "output_summary": json.dumps(parsed, default=str)[:500],
            "sql_queries": [],
            "llm_calls": [],
            "errors": [],
        })
    except Exception as e:
        stages_trace.append({
            "stage_name": "Query Parser",
            "duration_ms": 0,
            "errors": [str(e)],
            "sql_queries": [],
            "llm_calls": [],
        })

    # Stage 2: Data fetching
    if result["parsed_query"]:
        try:
            from src.agent.data_client import DataClientFactory
            from src.agent.data_fetcher import DataFetcher

            data_config = local_config.get("data", {})
            db = DataClientFactory.create(data_config)
            fetcher = DataFetcher(db)

            t0 = time.time()
            data = fetcher.fetch(result["parsed_query"])
            result["data_tables"] = data

            # Convert DataFrames to serializable summaries for trace
            data_summary = {}
            for k, v in data.items():
                if isinstance(v, pd.DataFrame):
                    data_summary[k] = f"{len(v)} rows, {len(v.columns)} cols"
                else:
                    data_summary[k] = str(type(v))

            stages_trace.append({
                "stage_name": "Data Fetcher",
                "duration_ms": (time.time() - t0) * 1000,
                "output_summary": json.dumps(data_summary)[:500],
                "sql_queries": [],
                "llm_calls": [],
                "errors": [],
            })
            db.close()
        except Exception as e:
            stages_trace.append({
                "stage_name": "Data Fetcher",
                "duration_ms": 0,
                "errors": [str(e)],
                "sql_queries": [],
                "llm_calls": [],
            })

    # Stages 3-5 (Attribution Reasoner, Grounding Check, Narrative Generator)
    # would go here once implemented. For now we note they are pending.
    for stage_name in ["Attribution Reasoner", "Grounding Check", "Narrative Generator"]:
        stages_trace.append({
            "stage_name": stage_name,
            "duration_ms": 0,
            "output_summary": "Not yet implemented",
            "sql_queries": [],
            "llm_calls": [],
            "errors": ["Stage not yet implemented"],
        })

    result["trace"] = {
        "trace_id": "ui-partial",
        "query": query,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "stages": stages_trace,
        "total_duration_ms": sum(s.get("duration_ms", 0) for s in stages_trace),
        "total_cost_usd": 0,
        "total_input_tokens": 0,
        "total_output_tokens": 0,
    }

    return result


# ---------------------------------------------------------------------------
# UI layout
# ---------------------------------------------------------------------------

# Example query buttons
st.markdown("##### Try an example query:")
example_cols = st.columns(len(EXAMPLE_QUERIES))
for i, eq in enumerate(EXAMPLE_QUERIES):
    with example_cols[i]:
        if st.button(eq[:50] + "...", key=f"example_{i}", use_container_width=True):
            st.session_state["query_input"] = eq

# Query input
query = st.text_area(
    "Your question:",
    value=st.session_state.get("query_input", ""),
    height=80,
    placeholder="e.g. Why did offer redemption rate increase 22% in India in late October 2024?",
    key="query_text_area",
)

# Check prerequisites
has_data = (SYNTHETIC_DIR / "daily_metrics.parquet").exists()

col_btn, col_status = st.columns([1, 3])
with col_btn:
    analyze_clicked = st.button(
        "Analyze",
        type="primary",
        use_container_width=True,
        disabled=not query.strip(),
    )

if not has_data:
    st.warning(
        "Synthetic data not found. Generate it first:\n\n"
        "```bash\npython -m src.data_generator.generator\n```"
    )

# ---------------------------------------------------------------------------
# Run analysis
# ---------------------------------------------------------------------------

if analyze_clicked and query.strip():
    st.markdown("---")

    with st.spinner("Running attribution pipeline..."):
        result = run_pipeline(query.strip())

    # Check for errors
    if result.get("error"):
        st.error(f"Pipeline error:\n```\n{result['error']}\n```")

    # ---- Parsed Query ----
    parsed = result.get("parsed_query")
    if parsed:
        with st.expander("Parsed Query Parameters", expanded=False):
            cols = st.columns(4)
            with cols[0]:
                st.markdown(f"**Metric:** `{parsed.get('metric', 'N/A')}`")
            with cols[1]:
                st.markdown(f"**Market:** `{parsed.get('market', 'ALL')}`")
            with cols[2]:
                st.markdown(f"**Category:** `{parsed.get('category', 'ALL')}`")
            with cols[3]:
                st.markdown(f"**Segment:** `{parsed.get('segment', 'ALL')}`")

            period = parsed.get("period", {})
            st.markdown(
                f"**Period:** {period.get('start_date', '?')} to "
                f"{period.get('end_date', '?')} "
                f"({period.get('description', '')})"
            )

            direction = parsed.get("direction")
            magnitude = parsed.get("magnitude")
            if direction or magnitude:
                st.markdown(
                    f"**Direction:** {direction or 'unspecified'} | "
                    f"**Magnitude:** {magnitude or 'unspecified'}"
                )

    # ---- Attribution Breakdown ----
    attribution_raw = result.get("attribution")
    # Unwrap nested structure: pipeline returns {"attribution": [...], "overall_confidence": ...}
    if isinstance(attribution_raw, dict):
        attribution_list = attribution_raw.get("attribution", [])
        overall_confidence = attribution_raw.get("overall_confidence", "N/A")
    elif isinstance(attribution_raw, list):
        attribution_list = attribution_raw
        overall_confidence = "N/A"
    else:
        attribution_list = []
        overall_confidence = None

    if attribution_list and len(attribution_list) > 0:
        st.markdown("### Attribution Breakdown")
        if overall_confidence and overall_confidence != "N/A":
            st.markdown(f"**Overall Confidence:** {overall_confidence}")

        from src.ui.components.charts import attribution_pie_chart, confidence_badge

        chart_col, detail_col = st.columns([1, 1])

        with chart_col:
            fig = attribution_pie_chart(attribution_list)
            st.plotly_chart(fig, use_container_width=True)

        with detail_col:
            for attr in attribution_list:
                cause = attr.get("cause", attr.get("name", "Unknown"))
                pct = attr.get("contribution_pct", attr.get("contribution", 0))
                # Handle pct as either 0-1 or 0-100 scale
                if isinstance(pct, (int, float)) and pct <= 1.0:
                    pct_display = pct * 100
                else:
                    pct_display = float(pct) if pct else 0
                conf = attr.get("confidence", "medium")
                evidence = attr.get("evidence", attr.get("reasoning", ""))
                attr_type = attr.get("type", attr.get("cause_type", ""))

                badge = confidence_badge(conf)
                st.markdown(
                    f"**{cause}** — {pct_display:.1f}% contribution | "
                    f"Confidence: {badge} | Type: `{attr_type}`"
                )
                if evidence:
                    st.caption(str(evidence)[:300])
                st.markdown("")

    elif attribution_raw is not None:
        st.info("Attribution analysis returned no results.")

    # ---- Narrative Report ----
    narrative = result.get("narrative")
    if narrative:
        st.markdown("### Narrative Report")
        st.markdown(narrative)

    # ---- Grounding Check ----
    grounding = result.get("grounding")
    if grounding and isinstance(grounding, dict):
        with st.expander("Grounding Verification", expanded=False):
            score = grounding.get("grounding_score", 0)
            st.markdown(f"**Grounding Score:** {score:.0%}")

            verified = grounding.get("verified_claims", [])
            if verified:
                for vc in verified:
                    status = vc.get("status", "UNKNOWN")
                    cause = vc.get("cause", "Unknown")
                    issues = vc.get("issues", [])
                    color = {
                        "VERIFIED": "green",
                        "PARTIALLY_VERIFIED": "orange",
                        "UNGROUNDED": "red",
                    }.get(status, "gray")
                    st.markdown(f":{color}[{status}] **{cause}**")
                    if issues:
                        for issue in issues:
                            st.caption(f"  - {issue}")

            critical = grounding.get("critical_issues", [])
            if critical:
                st.markdown("**Critical Issues:**")
                for ci in critical:
                    st.error(ci)

    # ---- Execution Trace ----
    trace = result.get("trace")
    if trace:
        st.markdown("---")
        from src.ui.components.trace_viewer import render_trace
        render_trace(trace)

    # ---- Data Tables ----
    data_tables = result.get("data_tables")
    if data_tables:
        st.markdown("---")
        from src.ui.components.trace_viewer import render_data_tables
        render_data_tables(data_tables)
