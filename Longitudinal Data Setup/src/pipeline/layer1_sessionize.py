"""Layer 1: collapse raw events into one row per user-session.

Input:  data/raw/events_*.parquet
Output: data/layer1_sessions/sessions.parquet

Per-session columns:
    user_id, session_id, market, category, segment, persona,
    started_at, ended_at, duration_s, event_count, distinct_events,
    events_seq (list of event types), had_install, had_purchase,
    had_offer_redeem, revenue_usd, date
"""

from __future__ import annotations

import time

import duckdb
import pandas as pd

from .. import config


def run() -> pd.DataFrame:
    t0 = time.time()
    con = duckdb.connect()
    con.execute("PRAGMA threads=4")

    raw_glob = str(config.RAW_DIR / "events_*.parquet")

    sql = f"""
    WITH raw AS (
        SELECT
            user_id,
            session_id,
            market,
            category,
            segment,
            persona,
            event_type,
            ts,
            revenue_usd
        FROM read_parquet('{raw_glob}')
    )
    SELECT
        user_id,
        session_id,
        ANY_VALUE(market)                          AS market,
        ANY_VALUE(category)                        AS category,
        ANY_VALUE(segment)                         AS segment,
        ANY_VALUE(persona)                         AS persona,
        MIN(ts)                                    AS started_at,
        MAX(ts)                                    AS ended_at,
        EPOCH(MAX(ts) - MIN(ts))                   AS duration_s,
        COUNT(*)                                   AS event_count,
        COUNT(DISTINCT event_type)                 AS distinct_events,
        LIST(event_type ORDER BY ts)               AS events_seq,
        MAX(CAST(event_type = 'app_install'  AS INT)) AS had_install,
        MAX(CAST(event_type = 'purchase'     AS INT)) AS had_purchase,
        MAX(CAST(event_type = 'offer_redeem' AS INT)) AS had_offer_redeem,
        SUM(revenue_usd)                           AS revenue_usd,
        CAST(MIN(ts) AS DATE)                      AS date
    FROM raw
    GROUP BY user_id, session_id
    """
    df = con.execute(sql).fetch_df()

    # Clamp odd durations (events with zero spread -> duration 0)
    df["duration_s"] = df["duration_s"].clip(lower=0)

    out_path = config.LAYER1_DIR / "sessions.parquet"
    df.to_parquet(out_path, index=False, compression="snappy")

    elapsed = time.time() - t0
    print(
        f"  [layer1] {len(df):,} sessions "
        f"-> {out_path.name} "
        f"({out_path.stat().st_size / 1024 / 1024:.1f} MB) "
        f"in {elapsed:.1f}s"
    )
    return df


if __name__ == "__main__":
    run()
