"""Layer 2: daily aggregates keyed by (date x market x category x segment).

Input:  data/layer1_sessions/sessions.parquet + data/raw/events_*.parquet
Output: data/layer2_daily/daily.parquet

Metrics per dimension per day:
    sessions, unique_users, events, install_count, purchase_count,
    offer_impressions, offer_clicks, offer_redeems, revenue_usd,
    conversion_rate, offer_ctr, offer_redemption_rate,
    avg_session_duration_s, avg_events_per_session
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

    sessions_path = str(config.LAYER1_DIR / "sessions.parquet")
    raw_glob = str(config.RAW_DIR / "events_*.parquet")

    # Event-level counters per (date, market, category, segment)
    event_sql = f"""
    SELECT
        CAST(ts AS DATE)                                   AS date,
        market, category, segment,
        COUNT(*)                                           AS events,
        SUM(CAST(event_type = 'app_install'      AS INT))  AS install_count,
        SUM(CAST(event_type = 'purchase'         AS INT))  AS purchase_count,
        SUM(CAST(event_type = 'offer_impression' AS INT))  AS offer_impressions,
        SUM(CAST(event_type = 'offer_click'      AS INT))  AS offer_clicks,
        SUM(CAST(event_type = 'offer_redeem'     AS INT))  AS offer_redeems,
        SUM(revenue_usd)                                   AS revenue_usd
    FROM read_parquet('{raw_glob}')
    GROUP BY 1, 2, 3, 4
    """
    events_df = con.execute(event_sql).fetch_df()

    # Session-level counters per (date, market, category, segment)
    sess_sql = f"""
    SELECT
        date,
        market, category, segment,
        COUNT(*)                      AS sessions,
        COUNT(DISTINCT user_id)       AS unique_users,
        AVG(duration_s)               AS avg_session_duration_s,
        AVG(event_count)              AS avg_events_per_session
    FROM read_parquet('{sessions_path}')
    GROUP BY 1, 2, 3, 4
    """
    sess_df = con.execute(sess_sql).fetch_df()

    # Join
    df = sess_df.merge(events_df, on=["date", "market", "category", "segment"], how="outer")
    df = df.fillna(0)
    df["date"] = pd.to_datetime(df["date"])

    # Derived ratio metrics (safe division)
    def safe_div(num, den):
        result = num.astype(float) / den.astype(float).replace(0, float("nan"))
        return result.fillna(0.0)

    df["conversion_rate"] = safe_div(df["purchase_count"], df["sessions"])
    df["install_rate"] = safe_div(df["install_count"], df["sessions"])
    df["offer_ctr"] = safe_div(df["offer_clicks"], df["offer_impressions"])
    df["offer_redemption_rate"] = safe_div(df["offer_redeems"], df["offer_clicks"])
    df["revenue_per_session"] = safe_div(df["revenue_usd"], df["sessions"])

    df = df.sort_values(["date", "market", "category", "segment"]).reset_index(drop=True)

    out_path = config.LAYER2_DIR / "daily.parquet"
    df.to_parquet(out_path, index=False, compression="snappy")

    elapsed = time.time() - t0
    print(
        f"  [layer2] {len(df):,} daily rows "
        f"-> {out_path.name} "
        f"({out_path.stat().st_size / 1024 / 1024:.1f} MB) "
        f"in {elapsed:.1f}s"
    )
    return df


if __name__ == "__main__":
    run()
