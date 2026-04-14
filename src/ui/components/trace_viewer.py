"""Execution trace viewer component."""

from __future__ import annotations

import json
import streamlit as st


def render_trace(trace: dict):
    """Render an execution trace in a collapsible format.

    Parameters
    ----------
    trace : dict
        Trace object as produced by ``Tracer.end_trace()``. Expected keys:
        - trace_id, query, timestamp
        - total_duration_ms, total_cost_usd, total_input_tokens, total_output_tokens
        - stages: list of stage dicts with stage_name, duration_ms,
          sql_queries, llm_calls, errors, input_summary, output_summary
    """
    if not trace:
        st.info("No execution trace available.")
        return

    # ---- Summary header ----
    st.markdown("#### Execution Trace")

    cols = st.columns(4)
    with cols[0]:
        duration = trace.get("total_duration_ms", 0)
        st.metric("Total Duration", f"{duration / 1000:.2f}s")
    with cols[1]:
        cost = trace.get("total_cost_usd", 0)
        st.metric("Total Cost", f"${cost:.4f}")
    with cols[2]:
        input_tok = trace.get("total_input_tokens", 0)
        st.metric("Input Tokens", f"{input_tok:,}")
    with cols[3]:
        output_tok = trace.get("total_output_tokens", 0)
        st.metric("Output Tokens", f"{output_tok:,}")

    trace_id = trace.get("trace_id", "unknown")
    timestamp = trace.get("timestamp", "")
    st.caption(f"Trace ID: `{trace_id}` | Timestamp: {timestamp}")

    # ---- Per-stage detail ----
    stages = trace.get("stages", [])
    if not stages:
        st.caption("No stages recorded in this trace.")
        return

    for i, stage in enumerate(stages):
        stage_name = stage.get("stage_name", f"Stage {i + 1}")
        duration_ms = stage.get("duration_ms", 0)
        errors = stage.get("errors", [])

        # Status indicator
        if errors:
            icon = "warning"
            status = f"{len(errors)} error(s)"
        else:
            icon = "white_check_mark"
            status = "OK"

        with st.expander(
            f":{icon}: **{stage_name}** — {duration_ms:.0f}ms — {status}",
            expanded=bool(errors),
        ):
            # Timing
            st.markdown(f"**Duration:** {duration_ms:.0f}ms ({duration_ms / 1000:.2f}s)")

            # Input / output summaries
            input_summary = stage.get("input_summary", "")
            output_summary = stage.get("output_summary", "")
            if input_summary:
                st.markdown("**Input:**")
                st.text(input_summary[:500])
            if output_summary:
                st.markdown("**Output:**")
                st.text(output_summary[:500])

            # SQL queries
            sql_queries = stage.get("sql_queries", [])
            if sql_queries:
                st.markdown(f"**SQL Queries ({len(sql_queries)}):**")
                for j, sql in enumerate(sql_queries):
                    st.code(sql, language="sql")

            # LLM calls
            llm_calls = stage.get("llm_calls", [])
            if llm_calls:
                st.markdown(f"**LLM Calls ({len(llm_calls)}):**")
                for call in llm_calls:
                    model = call.get("model", "unknown")
                    inp = call.get("input_tokens", 0)
                    out = call.get("output_tokens", 0)
                    cost_usd = call.get("cost_usd", 0)
                    latency = call.get("latency_ms", 0)
                    st.markdown(
                        f"- **{model}**: {inp:,} in / {out:,} out | "
                        f"${cost_usd:.4f} | {latency:.0f}ms"
                    )

            # Errors
            if errors:
                st.markdown("**Errors:**")
                for err in errors:
                    st.error(err)


def render_trace_compact(trace: dict):
    """Render a compact one-line summary of a trace.

    Useful for listing traces in a table or sidebar.
    """
    if not trace:
        return

    trace_id = trace.get("trace_id", "unknown")[:8]
    query = trace.get("query", "")[:60]
    duration = trace.get("total_duration_ms", 0) / 1000
    cost = trace.get("total_cost_usd", 0)
    n_stages = len(trace.get("stages", []))

    st.markdown(
        f"`{trace_id}` | {duration:.1f}s | ${cost:.4f} | "
        f"{n_stages} stages | _{query}_"
    )


def render_data_tables(data: dict):
    """Render fetched data tables in collapsible sections.

    Parameters
    ----------
    data : dict
        Keys are table names, values are either DataFrames or
        serializable data (list of dicts, etc.).
    """
    if not data:
        st.info("No data tables to display.")
        return

    st.markdown("#### Fetched Data Tables")

    import pandas as pd

    for table_name, table_data in data.items():
        if table_data is None:
            continue

        # Convert to DataFrame if not already
        if isinstance(table_data, pd.DataFrame):
            df = table_data
        elif isinstance(table_data, list):
            df = pd.DataFrame(table_data)
        elif isinstance(table_data, dict):
            df = pd.DataFrame([table_data])
        else:
            continue

        if df.empty:
            continue

        with st.expander(
            f"**{table_name.replace('_', ' ').title()}** ({len(df)} rows)",
            expanded=False,
        ):
            st.dataframe(
                df,
                use_container_width=True,
                height=min(400, 35 * len(df) + 38),
            )
