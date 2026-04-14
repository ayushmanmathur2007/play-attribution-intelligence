"""Page 4: Under the Hood -- system architecture, prompts, schema, config."""

from __future__ import annotations

import re
from pathlib import Path

import streamlit as st
import yaml
import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
PROMPTS_DIR = PROJECT_ROOT / "src" / "agent" / "prompts"
DATA_DIR = PROJECT_ROOT / "data" / "synthetic"

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Under the Hood | Play Attribution Intelligence",
    page_icon="🔧",
    layout="wide",
)

st.title("Under the Hood")
st.markdown(
    "Architecture, prompt templates, configuration, and data schemas."
)

# ---------------------------------------------------------------------------
# Tabs for organization
# ---------------------------------------------------------------------------

tab_arch, tab_prompts, tab_config, tab_schema, tab_howto = st.tabs([
    "Pipeline Architecture",
    "Prompt Templates",
    "Configuration",
    "Data Schema",
    "How We Built This",
])

# ============================================================================
# TAB 1: Pipeline Architecture
# ============================================================================

with tab_arch:
    st.markdown("### Attribution Pipeline -- 6 Stages")
    st.markdown(
        "The pipeline processes a natural language query through six sequential "
        "stages, each with a clear input/output contract."
    )

    st.code("""
    User Query (natural language)
          |
          v
    +------------------+
    | 1. Query Parser  |  NL -> structured params (metric, market, period, etc.)
    |   (LLM + regex)  |  Model: Claude Sonnet | Prompt: query_parser.txt
    +------------------+
          |
          v
    +------------------+
    | 2. Data Fetcher  |  Structured params -> SQL queries -> DataFrames
    |   (DuckDB)       |  Fetches: primary metric, adjacent metrics, cross-market,
    +------------------+  initiatives, confounders, change points
          |
          v
    +------------------------+
    | 3. Attribution         |  Data + context -> causal attribution JSON
    |    Reasoner (LLM)      |  Model: Claude Sonnet | Prompt: attribution_reasoner.txt
    +------------------------+  Output: causes[], contribution_pct, confidence, evidence
          |
          v
    +------------------------+
    | 4. Grounding Check     |  Attribution JSON + source data -> verified claims
    |    (LLM)               |  Model: Claude Haiku | Prompt: grounding_check.txt
    +------------------------+  Checks: initiative exists, dates align, direction matches
          |
          v
    +------------------------+
    | 5. Narrative Generator |  Verified attribution -> executive report (markdown)
    |    (LLM)               |  Model: Claude Sonnet | Prompt: narrative_generator.txt
    +------------------------+  Output: structured report with sections
          |
          v
    +------------------+
    | 6. Eval (offline)|  Agent output + ground truth -> 6-dimension scores
    |   (LLM-as-Judge) |  Model: Claude Haiku | Prompt: eval_judge.txt
    +------------------+

    Observability: Tracer logs every stage (timing, SQL, LLM calls, errors)
                   CostTracker records per-call token usage and costs
    """, language="text")

    st.markdown("---")
    st.markdown("### Stage Details")

    stages = [
        {
            "name": "1. Query Parser",
            "type": "LLM + regex fallback",
            "model": "Claude Sonnet",
            "input": "Natural language question",
            "output": "Structured params: metric, market, category, segment, period, direction, magnitude",
            "detail": (
                "Uses an LLM call with the query_parser.txt prompt to extract structured "
                "parameters. Falls back to regex-based extraction if the LLM call fails. "
                "Validates extracted values against the dimensions config (fuzzy matching "
                "for metrics and markets)."
            ),
        },
        {
            "name": "2. Data Fetcher",
            "type": "Deterministic (SQL)",
            "model": "N/A",
            "input": "Parsed query parameters",
            "output": "6 DataFrames: primary_data, adjacent_metrics, cross_market, initiatives, confounders, change_points",
            "detail": (
                "Constructs SQL queries dynamically based on parsed params. Uses a "
                "metric adjacency graph to pull correlated KPIs. Fetches YoY data for "
                "the primary metric. Widens the change point window by 7 days on each side."
            ),
        },
        {
            "name": "3. Attribution Reasoner",
            "type": "LLM (core reasoning)",
            "model": "Claude Sonnet",
            "input": "All fetched data + metric definitions + initiative details + seasonal baselines",
            "output": "JSON with causes[], contribution_pct, confidence, evidence, ruled_out[], data_quality_flags",
            "detail": (
                "The core analytical stage. The prompt enforces: list all plausible causes, "
                "estimate percentage contributions (must sum to ~100%), cite specific data, "
                "check for data quality issues first, note ruled-out causes, and check "
                "cross-market patterns."
            ),
        },
        {
            "name": "4. Grounding Check",
            "type": "LLM (fact-checking)",
            "model": "Claude Haiku",
            "input": "Attribution JSON + source data + initiative calendar + confounder log",
            "output": "verified_claims[] with status (VERIFIED/UNGROUNDED/PARTIALLY_VERIFIED), grounding_score",
            "detail": (
                "Verifies every attribution claim: does the initiative exist in the calendar? "
                "Were dates aligned with the query period? Did it target the right market/category? "
                "Is the direction consistent (boost initiative should not cause decline)?"
            ),
        },
        {
            "name": "5. Narrative Generator",
            "type": "LLM (writing)",
            "model": "Claude Sonnet",
            "input": "Verified attribution + movement context + metric definition",
            "output": "Markdown report with Executive Summary, Attribution Breakdown, Ruled Out, Data Quality Notes, Recommendations",
            "detail": (
                "Converts the structured attribution into an executive-ready narrative. "
                "Targets a General Manager audience. Uses specific numbers, direct language, "
                "and keeps the report under 500 words."
            ),
        },
        {
            "name": "6. Eval (Offline)",
            "type": "LLM-as-Judge",
            "model": "Claude Haiku",
            "input": "Agent output + ground truth attribution + golden narrative",
            "output": "Scores on 6 dimensions (attribution accuracy, cause ID, false attribution, data artifacts, narrative quality, grounding)",
            "detail": (
                "Runs offline against a golden dataset of pre-labeled metric movements. "
                "Uses a cheaper model (Haiku) as judge to score the agent on 6 weighted "
                "dimensions. Enables automated regression testing."
            ),
        },
    ]

    for s in stages:
        with st.expander(f"**{s['name']}** ({s['type']})"):
            cols = st.columns(3)
            with cols[0]:
                st.markdown(f"**Model:** {s['model']}")
            with cols[1]:
                st.markdown(f"**Input:** {s['input']}")
            with cols[2]:
                st.markdown(f"**Output:** {s['output']}")
            st.markdown(s["detail"])

    st.markdown("---")
    st.markdown("### Key Design Decisions")

    decisions = [
        (
            "DuckDB for local, BigQuery for GCP",
            "The DataClient abstraction allows swapping between DuckDB "
            "(reads parquet/CSV files directly, zero setup) for local dev "
            "and BigQuery for production GCP deployment. Same SQL dialects.",
        ),
        (
            "Anthropic for local, Vertex Gemini for GCP",
            "LLMClient abstraction wraps both Claude (Anthropic API) and "
            "Gemini (Vertex AI). Local dev uses Claude Sonnet for reasoning "
            "and Haiku for cheap judge/grounding calls.",
        ),
        (
            "Separate Grounding Check stage",
            "Rather than trusting the Attribution Reasoner's output directly, "
            "a separate LLM call fact-checks every claim against source data. "
            "This catches hallucinated initiatives, wrong dates, and "
            "direction mismatches.",
        ),
        (
            "Synthetic data with known ground truth",
            "The data generator injects initiative impacts, confounders, "
            "and seasonality with known parameters. This creates a ground "
            "truth for evaluating the agent's attribution accuracy.",
        ),
        (
            "LLM-as-Judge evaluation",
            "The eval framework uses a cheaper LLM (Haiku) to score the "
            "agent's output against ground truth on 6 dimensions, enabling "
            "automated regression testing without human annotators.",
        ),
    ]

    for title, detail in decisions:
        with st.expander(title):
            st.markdown(detail)


# ============================================================================
# TAB 2: Prompt Templates
# ============================================================================

with tab_prompts:
    st.markdown("### Prompt Templates")
    st.markdown(
        "Each LLM stage uses a dedicated prompt template. Select one below "
        "to view its contents."
    )

    # Discover prompt files
    prompt_files: dict[str, Path] = {}
    if PROMPTS_DIR.exists():
        for f in sorted(PROMPTS_DIR.glob("*.txt")):
            prompt_files[f.stem] = f

    if not prompt_files:
        st.warning(
            f"No prompt templates found in `{PROMPTS_DIR}`. "
            "Expected .txt files like query_parser.txt, attribution_reasoner.txt, etc."
        )
    else:
        # Mapping from filename to stage description
        prompt_descriptions = {
            "query_parser": "Stage 1: Parse natural language query into structured parameters",
            "attribution_reasoner": "Stage 3: Analyze data and produce causal attribution",
            "grounding_check": "Stage 4: Fact-check attribution claims against source data",
            "narrative_generator": "Stage 5: Convert verified attribution into executive report",
            "eval_judge": "Stage 6: Score agent output against ground truth (LLM-as-Judge)",
        }

        selected_prompt = st.selectbox(
            "Select template",
            options=list(prompt_files.keys()),
            format_func=lambda x: f"{x} -- {prompt_descriptions.get(x, '')}",
        )

        if selected_prompt:
            prompt_path = prompt_files[selected_prompt]
            content = prompt_path.read_text()

            info_cols = st.columns(3)
            with info_cols[0]:
                st.markdown(f"**File:** `src/agent/prompts/{prompt_path.name}`")
            with info_cols[1]:
                st.markdown(f"**Stage:** {prompt_descriptions.get(selected_prompt, 'N/A')}")
            with info_cols[2]:
                st.markdown(f"**Size:** {len(content)} chars, {len(content.split())} words")

            st.code(content, language="text", line_numbers=True)

            # Show template variables
            variables = sorted(set(re.findall(r"\{(\w+)\}", content)))
            if variables:
                st.markdown("**Template variables used in this prompt:**")
                var_cols = st.columns(min(len(variables), 4))
                for i, v in enumerate(variables):
                    with var_cols[i % len(var_cols)]:
                        st.code(f"{{{v}}}")


# ============================================================================
# TAB 3: Configuration
# ============================================================================

with tab_config:
    st.markdown("### Configuration Files")
    st.markdown(
        "Side-by-side comparison of local development and GCP deployment configs."
    )

    col_local, col_gcp = st.columns(2)

    with col_local:
        st.markdown("#### local.yaml (current)")
        local_path = CONFIG_DIR / "local.yaml"
        if local_path.exists():
            st.code(local_path.read_text(), language="yaml", line_numbers=True)
        else:
            st.warning("local.yaml not found.")

    with col_gcp:
        st.markdown("#### gcp.yaml (target)")
        gcp_path = CONFIG_DIR / "gcp.yaml"
        if gcp_path.exists():
            st.code(gcp_path.read_text(), language="yaml", line_numbers=True)
        else:
            st.warning("gcp.yaml not found.")

    st.markdown("---")
    st.markdown("### What Changes Between Local and GCP")
    st.markdown("""
    | Component | Local | GCP | Code Change |
    |-----------|-------|-----|-------------|
    | LLM | Claude API | Vertex AI Gemini | 1 config value |
    | Data | DuckDB on parquet | BigQuery | 1 config value + minor SQL |
    | File Storage | Local filesystem | GCS | Swap file paths |
    | Tracing | JSON files | Cloud Trace | Swap Tracer class |
    | UI Hosting | localhost:8501 | Cloud Run | Docker deploy |
    | **Prompts** | **Same** | **Same** | **No change** |
    | **Agent Logic** | **Same** | **Same** | **No change** |
    | **Eval Logic** | **Same** | **Same** | **No change** |
    """)

    st.markdown("---")
    st.markdown("#### dimensions.yaml (metrics, markets, categories, segments)")

    dims_path = CONFIG_DIR / "dimensions.yaml"
    if dims_path.exists():
        dims = yaml.safe_load(dims_path.read_text()) or {}

        summary_cols = st.columns(4)
        with summary_cols[0]:
            st.metric("Markets", len(dims.get("markets", [])))
        with summary_cols[1]:
            st.metric("Categories", len(dims.get("categories", [])))
        with summary_cols[2]:
            st.metric("Segments", len(dims.get("segments", [])))
        with summary_cols[3]:
            st.metric("Metrics", len(dims.get("metrics", [])))

        with st.expander("Markets (15)"):
            markets = dims.get("markets", [])
            if markets:
                st.dataframe(pd.DataFrame(markets), use_container_width=True, hide_index=True)

        with st.expander("Categories (12)"):
            categories = dims.get("categories", [])
            if categories:
                st.dataframe(pd.DataFrame(categories), use_container_width=True, hide_index=True)

        with st.expander("Segments (10)"):
            segments = dims.get("segments", [])
            if segments:
                st.dataframe(pd.DataFrame(segments), use_container_width=True, hide_index=True)

        with st.expander("Metrics (25 KPIs)"):
            metrics = dims.get("metrics", [])
            if metrics:
                met_rows = []
                for m in metrics:
                    base = m.get("base_values", {})
                    met_rows.append({
                        "ID": m.get("id", ""),
                        "Name": m.get("name", ""),
                        "Unit": m.get("unit", ""),
                        "Definition": str(m.get("definition", "")).strip()[:100],
                        "Tier 1 Base": base.get("tier_1", ""),
                        "Tier 2 Base": base.get("tier_2", ""),
                        "Tier 3 Base": base.get("tier_3", ""),
                    })
                st.dataframe(pd.DataFrame(met_rows), use_container_width=True, hide_index=True, height=600)

        with st.expander("Full YAML (raw)"):
            st.code(dims_path.read_text(), language="yaml")
    else:
        st.warning("dimensions.yaml not found.")


# ============================================================================
# TAB 4: Data Schema
# ============================================================================

with tab_schema:
    st.markdown("### Data Schema")
    st.markdown(
        "Overview of all data tables, their columns, and sample rows. "
        "Data lives in `data/synthetic/` as Parquet and CSV files."
    )

    # Define expected tables and their files
    table_files = {
        "daily_metrics": ("daily_metrics.parquet", "parquet"),
        "journey_aggregates": ("journey_aggregates.parquet", "parquet"),
        "initiative_calendar": ("initiative_calendar.csv", "csv"),
        "offer_catalog": ("offer_catalog.csv", "csv"),
        "metric_movements_golden": ("metric_movements_golden.csv", "csv"),
        "change_points": ("change_points.csv", "csv"),
        "confounder_log": ("confounder_log.csv", "csv"),
    }

    found_tables: list[tuple] = []
    missing_tables: list[tuple] = []

    for table_name, (filename, fmt) in table_files.items():
        path = DATA_DIR / filename
        if path.exists():
            found_tables.append((table_name, filename, fmt, path))
        else:
            missing_tables.append((table_name, filename))

    # Summary
    status_cols = st.columns(3)
    with status_cols[0]:
        st.metric("Tables Found", len(found_tables))
    with status_cols[1]:
        st.metric("Tables Missing", len(missing_tables))
    with status_cols[2]:
        st.metric("Total Expected", len(table_files))

    if missing_tables:
        with st.expander(f"{len(missing_tables)} missing tables"):
            for name, fname in missing_tables:
                st.markdown(f"- `{name}` (`{fname}`)")
            st.info("Run `python -m src.data_generator.generator` to generate all tables.")

    # Show each found table
    for table_name, filename, fmt, path in found_tables:
        with st.expander(f"**{table_name}** (`{filename}`)"):
            try:
                if fmt == "parquet":
                    df = pd.read_parquet(path)
                else:
                    df = pd.read_csv(path)

                st.markdown(f"**Shape:** {df.shape[0]:,} rows x {df.shape[1]} columns")

                # Schema info
                col_info = []
                for col in df.columns:
                    sample_val = ""
                    if not df[col].dropna().empty:
                        sample_val = str(df[col].dropna().iloc[0])[:60]
                    col_info.append({
                        "Column": col,
                        "Type": str(df[col].dtype),
                        "Non-Null": f"{df[col].notna().sum():,}",
                        "Null": f"{df[col].isna().sum():,}",
                        "Unique": f"{df[col].nunique():,}",
                        "Sample": sample_val,
                    })
                st.dataframe(pd.DataFrame(col_info), use_container_width=True, hide_index=True)

                # Sample rows
                st.markdown("**Sample rows (first 5):**")
                st.dataframe(df.head(5), use_container_width=True, hide_index=True)

            except Exception as e:
                st.error(f"Error reading {filename}: {e}")


# ============================================================================
# TAB 5: How We Built This
# ============================================================================

with tab_howto:
    st.markdown("### How We Built This")
    st.markdown(
        """
        **Play Attribution Intelligence** is an LLM-powered system that
        automatically explains *why* Google Play loyalty and offer metrics
        moved.

        ---

        #### The Problem

        When a metric like Offer Redemption Rate changes significantly, product
        managers need to understand *why*. Was it an initiative we launched? A
        seasonal pattern? An external event? A data pipeline issue? Answering
        this question manually takes hours of analyst time, involves querying
        multiple data sources, and requires deep institutional knowledge about
        what initiatives were running and when.

        #### Our Approach

        We built a 6-stage LLM agent pipeline that mimics how a senior analyst
        would investigate a metric movement:

        1. **Parse the question** -- Extract the metric, market, time period,
           and direction from a natural language query.
        2. **Gather evidence** -- Pull the target metric data, adjacent metrics
           (correlated KPIs), cross-market comparisons, active initiatives,
           known confounders, and detected change points.
        3. **Reason about causes** -- Feed all the evidence to an LLM with a
           structured prompt that enforces causal reasoning, contribution
           percentages, and evidence citations.
        4. **Fact-check the claims** -- A separate LLM call verifies every
           attribution claim against the source data (initiative dates, market
           targeting, direction consistency).
        5. **Write the report** -- Generate an executive-ready narrative with
           sections for summary, breakdown, ruled-out causes, data quality
           notes, and recommendations.
        6. **Evaluate** -- An offline LLM-as-Judge framework scores the agent
           against ground truth on 6 dimensions.

        #### Key Technical Choices

        - **Claude Sonnet** for reasoning (Stages 1, 3, 5) -- good balance
          of capability and cost.
        - **Claude Haiku** for grounding and eval (Stages 4, 6) -- cheap,
          fast, sufficient for fact-checking.
        - **DuckDB** for local data queries -- reads Parquet/CSV directly,
          zero infrastructure.
        - **Synthetic data with planted ground truth** -- the data generator
          injects known initiative impacts, seasonal patterns, and confounders,
          so we can measure attribution accuracy objectively.
        - **Dual-cloud portability** -- `DataClient` and `LLMClient`
          abstractions allow swapping between Anthropic/DuckDB (local) and
          Vertex Gemini/BigQuery (GCP) with a config change.

        #### Synthetic Data Generation

        The data generator creates 18 months of daily metrics across
        15 markets, 12 categories, 10 segments, and 25 KPIs. The value
        formula combines:

        - **Base values** (tier-specific)
        - **Market modifiers** (GDP-scaled)
        - **Category modifiers** (Games vs Apps)
        - **Structural trends** (growth/decline over time)
        - **Seasonality** (weekly, monthly, holiday patterns)
        - **Initiative impacts** (trapezoidal ramp-up/ramp-down)
        - **Confounder effects** (outages, competitor launches, policy changes)
        - **Noise** (Gaussian, 2% of base value)

        #### Evaluation Framework

        The eval suite runs the agent on golden test cases (metric movements
        with known ground truth attributions) and scores the output on:

        | Dimension | Weight | Description |
        |-----------|--------|-------------|
        | Attribution Accuracy | 30% | Contribution % closeness to ground truth |
        | Cause Identification | 25% | F1 score on finding real causes |
        | False Attribution | 15% | Penalty for hallucinated causes |
        | Data Artifact Detection | 10% | Identifying data quality vs real movements |
        | Narrative Quality | 10% | Clarity, structure, actionability (LLM judge) |
        | Factual Grounding | 10% | All claims traceable to source data |

        #### Build Timeline

        **Days 1-2**: Generated 18 months of synthetic data mirroring Google
        Play's Loyalty and Offers domain. Modeled 25 KPIs across 15 markets,
        12 categories, and 10 user segments with realistic seasonality, 18
        marketing initiatives with trapezoidal impact envelopes, and 8 external
        confounders including data pipeline issues and competitor launches.

        **Days 3-4**: Built the 6-stage attribution agent pipeline. The core
        innovation is in Stage 3 (Attribution Reasoner) -- a carefully
        engineered prompt that decomposes metric movements into constituent
        causes with percentage attributions, evidence citations, and confidence
        levels.

        **Days 5-6**: Built the evaluation framework with 6 scoring dimensions.
        The golden dataset contains 32+ pre-labeled movement cases (EASY,
        MEDIUM, HARD) with ground-truth attributions baked in during data
        generation.

        **Days 7-8**: Iterated on prompts based on eval failures. Key
        improvements: better handling of overlapping initiatives, explicit data
        quality checking before business attribution, and cross-market
        comparison for separating local vs global effects.

        **Days 9-10**: Built this Streamlit UI, polished the demo flow, and
        prepared the GCP migration path.

        ---

        #### Technology Stack

        | Component | Technology |
        |-----------|-----------|
        | Agent LLM | Claude Sonnet 4 (Anthropic API) |
        | Judge LLM | Claude Haiku 4.5 (Anthropic API) |
        | Local Data | DuckDB + Parquet/CSV |
        | GCP Data | BigQuery (stub) |
        | GCP LLM | Vertex Gemini (stub) |
        | UI | Streamlit 1.44 |
        | Charts | Plotly 6.0 |
        | Eval | Custom framework + LLM-as-Judge |
        | Observability | JSON tracing + cost tracking |
        | Language | Python 3.12 |
        """
    )
