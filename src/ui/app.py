"""Play Attribution Intelligence -- Streamlit main app."""

import streamlit as st
from pathlib import Path

# ---------------------------------------------------------------------------
# Page configuration (must be the first Streamlit command)
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Play Attribution Intelligence",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
SYNTHETIC_DIR = DATA_DIR / "synthetic"
EVAL_DIR = DATA_DIR / "eval"
CONFIG_DIR = PROJECT_ROOT / "config"

# ---------------------------------------------------------------------------
# Sidebar — branding & system status
# ---------------------------------------------------------------------------

with st.sidebar:
    st.markdown(
        """
        <div style="text-align:center; padding-bottom:0.5rem;">
            <h2 style="margin:0; letter-spacing:-0.5px;">Play Attribution<br/>Intelligence</h2>
            <p style="color:#94a3b8; font-size:0.85rem; margin-top:0.25rem;">
                by LatentView Analytics
            </p>
        </div>
        <hr style="border-color:#334155; margin:0.5rem 0 1rem 0;"/>
        """,
        unsafe_allow_html=True,
    )

    # Navigation
    st.markdown("### Pages")
    st.page_link("pages/1_attribution_agent.py", label="Attribution Agent", icon="🔍")
    st.page_link("pages/2_eval_dashboard.py", label="Eval Dashboard", icon="📊")
    st.page_link("pages/3_data_explorer.py", label="Data Explorer", icon="📈")
    st.page_link("pages/4_under_the_hood.py", label="Under the Hood", icon="🔧")

    st.markdown("---")

    # System status
    st.markdown("### System Status")

    # Check synthetic data
    has_daily_metrics = (SYNTHETIC_DIR / "daily_metrics.parquet").exists()
    has_initiative_cal = (SYNTHETIC_DIR / "initiative_calendar.csv").exists()
    has_change_points = (SYNTHETIC_DIR / "change_points.csv").exists()
    has_golden = (SYNTHETIC_DIR / "metric_movements_golden.csv").exists()

    data_count = sum([has_daily_metrics, has_initiative_cal, has_change_points, has_golden])
    if data_count == 4:
        st.success(f"Synthetic data: {data_count}/4 files present")
    elif data_count > 0:
        st.warning(f"Synthetic data: {data_count}/4 files present")
    else:
        st.error("Synthetic data: not generated yet")

    # Check eval results
    has_eval_results = (EVAL_DIR / "eval_results.json").exists()
    has_eval_summary = (EVAL_DIR / "eval_summary.json").exists()
    if has_eval_results and has_eval_summary:
        st.success("Eval results: available")
    else:
        st.info("Eval results: not run yet")

    # Check config
    has_local_config = (CONFIG_DIR / "local.yaml").exists()
    has_dimensions = (CONFIG_DIR / "dimensions.yaml").exists()
    if has_local_config and has_dimensions:
        st.success("Config: loaded")
    else:
        st.warning("Config: missing files")

    st.markdown("---")
    st.caption("v0.1.0 | Powered by Claude + DuckDB")

# ---------------------------------------------------------------------------
# Main content — landing page
# ---------------------------------------------------------------------------

st.title("Play Attribution Intelligence")
st.markdown(
    """
    Welcome to the **Google Play Loyalty & Offers Attribution System**.
    This tool uses an LLM-powered agent pipeline to automatically explain
    *why* metrics moved -- attributing changes to initiatives, seasonality,
    external events, and organic trends.
    """
)

st.markdown("---")

# Quick-start cards
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.markdown(
        """
        #### Ask the Agent
        Type a natural language question about any metric
        movement and get an analyst-quality attribution report.
        """
    )
    st.page_link("pages/1_attribution_agent.py", label="Go to Attribution Agent", icon="🔍")

with col2:
    st.markdown(
        """
        #### Eval Dashboard
        See how the agent performs across difficulty levels,
        with per-dimension scoring and failure analysis.
        """
    )
    st.page_link("pages/2_eval_dashboard.py", label="Go to Eval Dashboard", icon="📊")

with col3:
    st.markdown(
        """
        #### Explore Data
        Browse the synthetic dataset interactively -- plot
        metrics, overlay initiatives, find change points.
        """
    )
    st.page_link("pages/3_data_explorer.py", label="Go to Data Explorer", icon="📈")

with col4:
    st.markdown(
        """
        #### Under the Hood
        See the pipeline architecture, prompt templates,
        configs, and data schemas.
        """
    )
    st.page_link("pages/4_under_the_hood.py", label="Go to Under the Hood", icon="🔧")

# Setup instructions if data is missing
if not has_daily_metrics:
    st.markdown("---")
    st.warning("Synthetic data has not been generated yet. Run the following to get started:")
    st.code(
        "# From the project root:\npython -m src.data_generator.generator\n\n"
        "# Or if you have a Makefile:\nmake data",
        language="bash",
    )

if not has_eval_results:
    st.info(
        "Evaluation has not been run yet. After generating data, run: "
        "`make eval` or `python -m src.eval`"
    )
