# PRD: Loyalty & Offers Attribution Intelligence System
## Google Play DS&A × LatentView — Workshop POV Prototype

**Version**: 1.0
**Author**: [Your Name], Senior Client Partner
**Target Audience**: Developer / Claude Code / Engineering Lead
**Build Timeline**: 10 days (local) → 2 days (GCP port)
**Last Updated**: April 2026

---

## 1. PURPOSE & CONTEXT

### 1.1 What Is This

A fully functional prototype of an AI-powered metric attribution system for Google Play's Loyalty & Offers domain. It ingests synthetic data that mirrors real Play Store dimensions and KPIs, detects metric movements, attributes them to known initiatives/campaigns with causal reasoning, and evaluates its own accuracy against baked-in ground truth.

### 1.2 Why We're Building It

We're meeting with Google Play's DS&A team for a workshop. Instead of slides, we're showing a working system. This prototype demonstrates:
- The agent architecture we'd deploy in a real POC
- A rigorous evaluation framework (their team has no formal eval today)
- The synthetic-to-real data transition pattern (de-risks their data access bottleneck)
- The "AI-ready data layer" concept (pre-computed trends, change points, journey archetypes)

### 1.3 Success Criteria for the Prototype

| Criteria | Measurable Target |
|----------|-------------------|
| Agent produces attribution output for any metric movement query | 100% of queries return structured output |
| Agent accuracy on golden dataset | >65% attribution accuracy (MVP threshold) |
| Eval framework produces automated scorecard | Scorecard generated on every agent run |
| End-to-end demo runs in <60 seconds per query | Timed and verified |
| Workshop audience can interact with the system live | Web UI accessible, no CLI needed |
| System is portable to GCP/Vertex AI | All GCP boundaries clearly abstracted |

### 1.4 What This Is NOT

- Not production-ready (no auth, no multi-tenancy, no SLA)
- Not connected to real Google Play data (synthetic only)
- Not optimized for cost or latency at scale
- Not a replacement for their internal tools — it's a POV of what we'd build together

---

## 2. ARCHITECTURE OVERVIEW

### 2.1 System Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                        WEB UI (Streamlit)                       │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────────────┐   │
│  │ Query Input   │  │ Agent Output │  │ Eval Dashboard     │   │
│  │ (natural      │  │ (attribution │  │ (scorecard,        │   │
│  │  language)    │  │  narrative)  │  │  accuracy trends)  │   │
│  └──────┬───────┘  └──────▲───────┘  └────────▲───────────┘   │
│         │                 │                    │               │
└─────────┼─────────────────┼────────────────────┼───────────────┘
          │                 │                    │
┌─────────▼─────────────────┴────────────────────┴───────────────┐
│                     ORCHESTRATOR (FastAPI)                      │
│                                                                │
│  ┌────────────────────────────────────────────────────────┐   │
│  │                  AGENT PIPELINE                         │   │
│  │                                                        │   │
│  │  ┌──────────┐  ┌───────────┐  ┌───────────────────┐  │   │
│  │  │ 1. Query │→ │ 2. Data   │→ │ 3. Context        │  │   │
│  │  │ Parser   │  │ Fetcher   │  │ Enricher          │  │   │
│  │  │          │  │ (SQL on   │  │ (metric defs,     │  │   │
│  │  │ (extract │  │ DuckDB)   │  │  initiatives,     │  │   │
│  │  │ metric,  │  │           │  │  seasonality,     │  │   │
│  │  │ period,  │  │           │  │  change points)   │  │   │
│  │  │ market)  │  │           │  │                   │  │   │
│  │  └──────────┘  └───────────┘  └─────────┬─────────┘  │   │
│  │                                          │            │   │
│  │  ┌──────────┐  ┌───────────┐  ┌─────────▼─────────┐  │   │
│  │  │ 6. Narr- │← │ 5. Fact   │← │ 4. Attribution   │  │   │
│  │  │ ative    │  │ Grounding │  │ Reasoner          │  │   │
│  │  │ Generator│  │ Check     │  │ (LLM: causal     │  │   │
│  │  │ (LLM)   │  │ (verify   │  │  decomposition)  │  │   │
│  │  │          │  │ vs data)  │  │                   │  │   │
│  │  └──────────┘  └───────────┘  └───────────────────┘  │   │
│  │                                                        │   │
│  └────────────────────────────────────────────────────────┘   │
│                                                                │
│  ┌────────────────────────────────────────────────────────┐   │
│  │                 EVAL ENGINE                             │   │
│  │                                                        │   │
│  │  ┌──────────┐  ┌───────────┐  ┌───────────────────┐  │   │
│  │  │ Golden   │  │ Scorer    │  │ LLM-as-Judge      │  │   │
│  │  │ Dataset  │  │ (accuracy,│  │ (narrative quality,│  │   │
│  │  │ Manager  │  │ precision,│  │  completeness,     │  │   │
│  │  │          │  │ recall)   │  │  actionability)    │  │   │
│  │  └──────────┘  └───────────┘  └───────────────────┘  │   │
│  │                                                        │   │
│  └────────────────────────────────────────────────────────┘   │
│                                                                │
│  ┌────────────────────────────────────────────────────────┐   │
│  │               OBSERVABILITY                             │   │
│  │  Trace logging │ Latency tracking │ Cost per query     │   │
│  └────────────────────────────────────────────────────────┘   │
│                                                                │
└────────────────────────────────────────────────────────────────┘
          │
┌─────────▼──────────────────────────────────────────────────────┐
│                      DATA LAYER                                │
│                                                                │
│  ┌────────────┐  ┌──────────────┐  ┌────────────────────┐    │
│  │ DuckDB     │  │ Knowledge    │  │ Trace Store        │    │
│  │ (synthetic │  │ Store        │  │ (JSON logs)        │    │
│  │  data,     │  │ (metric      │  │                    │    │
│  │  parquet)  │  │  definitions,│  │ → GCP: BigQuery    │    │
│  │            │  │  initiative  │  │   + Cloud Trace    │    │
│  │ → GCP:     │  │  calendar)   │  │                    │    │
│  │   BigQuery │  │              │  │                    │    │
│  └────────────┘  │ → GCP:       │  └────────────────────┘    │
│                  │   Firestore / │                             │
│                  │   Cloud SQL   │                             │
│                  └──────────────┘                              │
│                                                                │
└────────────────────────────────────────────────────────────────┘
```

### 2.2 Technology Stack (Local)

| Component | Technology | Why |
|-----------|-----------|-----|
| Language | Python 3.11+ | Team familiarity, ecosystem |
| Data Engine | DuckDB | SQL on parquet files, zero infra, blazing fast for analytical queries, easy swap to BigQuery later |
| LLM (primary) | Anthropic Claude API (claude-sonnet-4-20250514) | Best reasoning for complex attribution. Used for agent pipeline |
| LLM (eval judge) | Anthropic Claude API (claude-haiku-4-5-20251001) | Cost-efficient for automated eval scoring at scale |
| API Framework | FastAPI | Async, fast, clean. Easy containerization |
| Web UI | Streamlit | Fastest path to interactive demo. Good enough for workshop |
| Data Generation | Python (pandas, numpy, scipy) | Full control over synthetic data patterns |
| Observability | Custom JSON trace logger | Simple, portable. Swap for Cloud Trace on GCP |
| Containerization | Docker + docker-compose | Local dev parity with cloud deployment |

### 2.3 Technology Stack (GCP Target — Port Later)

| Local Component | GCP Equivalent | Migration Effort |
|----------------|----------------|-----------------|
| DuckDB on parquet | BigQuery | Rewrite SQL dialect (minor), change connection string |
| Claude API | Vertex AI Gemini API | Swap LLM client class. Prompt templates stay same |
| FastAPI | Cloud Run | Containerize (already Docker), deploy |
| Streamlit | Cloud Run (separate service) | Same container, different port mapping |
| JSON trace logs | Cloud Trace + Cloud Logging | Swap logger implementation |
| Local file storage | Cloud Storage (GCS) | Swap file paths to gs:// URIs |
| Eval pipeline | Vertex AI Evaluation Service | Adapt eval harness to use Vertex eval SDK |
| N/A | Vertex AI Agent Builder | Optional: migrate agent to native Vertex agent framework |

---

## 3. COMPONENT SPECIFICATIONS

### 3.1 DATA GENERATOR (`src/data_generator/`)

#### 3.1.1 Purpose
Generate 18 months of realistic synthetic data for the Loyalty & Offers domain of Google Play.

#### 3.1.2 Output Files

| File | Format | Description | Approx Size |
|------|--------|-------------|-------------|
| `daily_metrics.parquet` | Parquet | Core metrics table: date × market × category × segment × metric | ~50-100MB |
| `initiative_calendar.csv` | CSV | All campaigns/offers with dates, targets, known impacts | <1MB |
| `offer_catalog.csv` | CSV | Offer definitions with eligibility rules | <1MB |
| `metric_movements_golden.csv` | CSV | Detected movements with ground-truth attribution | <1MB |
| `change_points.csv` | CSV | Pre-computed structural change points per metric | <1MB |
| `journey_aggregates.parquet` | Parquet | Weekly user journey archetype distributions | ~5MB |
| `metric_definitions.json` | JSON | KPI definitions with business logic and context | <1MB |
| `seasonal_patterns.json` | JSON | Pre-computed seasonal baselines per metric × market | <1MB |
| `confounder_log.csv` | CSV | External events (competitor launches, policy changes, data issues) | <1MB |

#### 3.1.3 Dimensions

**Markets (15)**:
```python
MARKETS = [
    {"id": "US", "name": "United States", "region": "NAM", "tier": 1, "timezone": "America/New_York"},
    {"id": "GB", "name": "United Kingdom", "region": "EUR", "tier": 1, "timezone": "Europe/London"},
    {"id": "DE", "name": "Germany", "region": "EUR", "tier": 1, "timezone": "Europe/Berlin"},
    {"id": "JP", "name": "Japan", "region": "APAC", "tier": 1, "timezone": "Asia/Tokyo"},
    {"id": "KR", "name": "South Korea", "region": "APAC", "tier": 1, "timezone": "Asia/Seoul"},
    {"id": "BR", "name": "Brazil", "region": "LATAM", "tier": 2, "timezone": "America/Sao_Paulo"},
    {"id": "IN", "name": "India", "region": "APAC", "tier": 2, "timezone": "Asia/Kolkata"},
    {"id": "MX", "name": "Mexico", "region": "LATAM", "tier": 2, "timezone": "America/Mexico_City"},
    {"id": "ID", "name": "Indonesia", "region": "APAC", "tier": 2, "timezone": "Asia/Jakarta"},
    {"id": "TR", "name": "Turkey", "region": "EMEA", "tier": 2, "timezone": "Europe/Istanbul"},
    {"id": "RU", "name": "Russia", "region": "EMEA", "tier": 3, "timezone": "Europe/Moscow"},
    {"id": "NG", "name": "Nigeria", "region": "AFR", "tier": 3, "timezone": "Africa/Lagos"},
    {"id": "PH", "name": "Philippines", "region": "APAC", "tier": 3, "timezone": "Asia/Manila"},
    {"id": "EG", "name": "Egypt", "region": "EMEA", "tier": 3, "timezone": "Africa/Cairo"},
    {"id": "VN", "name": "Vietnam", "region": "APAC", "tier": 3, "timezone": "Asia/Ho_Chi_Minh"},
]
```

**App Categories (12)**:
```python
CATEGORIES = [
    {"id": "GAM_CAS", "name": "Casual Games", "vertical": "Games"},
    {"id": "GAM_MID", "name": "Midcore Games", "vertical": "Games"},
    {"id": "GAM_HC", "name": "Hardcore Games", "vertical": "Games"},
    {"id": "GAM_HYP", "name": "Hypercasual Games", "vertical": "Games"},
    {"id": "APP_SOC", "name": "Social & Communication", "vertical": "Apps"},
    {"id": "APP_ENT", "name": "Entertainment & Streaming", "vertical": "Apps"},
    {"id": "APP_PRD", "name": "Productivity & Tools", "vertical": "Apps"},
    {"id": "APP_FIN", "name": "Finance & Fintech", "vertical": "Apps"},
    {"id": "APP_SHP", "name": "Shopping & Commerce", "vertical": "Apps"},
    {"id": "APP_EDU", "name": "Education", "vertical": "Apps"},
    {"id": "APP_HLT", "name": "Health & Fitness", "vertical": "Apps"},
    {"id": "APP_TRV", "name": "Travel & Navigation", "vertical": "Apps"},
]
```

**User Segments (10)**:
```python
SEGMENTS = [
    {"id": "NEW_0_7", "name": "New Users (0-7d)", "base_size_pct": 0.05},
    {"id": "NEW_8_30", "name": "New Users (8-30d)", "base_size_pct": 0.08},
    {"id": "ACT_HIGH", "name": "High-Value Active", "base_size_pct": 0.10},
    {"id": "ACT_MED", "name": "Medium-Value Active", "base_size_pct": 0.25},
    {"id": "ACT_LOW", "name": "Low-Value Active", "base_size_pct": 0.15},
    {"id": "ACT_FREE", "name": "Active Free Users", "base_size_pct": 0.12},
    {"id": "LAP_30_90", "name": "Lapsed (30-90d)", "base_size_pct": 0.10},
    {"id": "LAP_90", "name": "Churned (90+d)", "base_size_pct": 0.08},
    {"id": "PP_ACTIVE", "name": "Play Points Active", "base_size_pct": 0.05},
    {"id": "PP_DORMANT", "name": "Play Points Dormant", "base_size_pct": 0.02},
]
```

**Metrics (25 KPIs)** — see Section 3.1.4

#### 3.1.4 Metric Definitions (Complete)

```python
METRICS = [
    # === Offer Performance ===
    {
        "name": "offer_impression_count",
        "definition": "Number of unique users who were shown at least one offer in the period",
        "unit": "count",
        "base_values": {"tier_1": 150000, "tier_2": 300000, "tier_3": 80000},
        "business_logic": "Counted per user per day. If a user sees 3 offers, counted as 1 impression.",
        "source_tables": ["play_loyalty.offer_impressions_v2"],
        "alert_threshold": "±15% WoW",
        "typical_range": "50K-500K per market per day",
        "caveats": "Does not include offers shown in push notifications; only in-app surfaces."
    },
    {
        "name": "offer_ctr",
        "definition": "Percentage of users who clicked on an offer after seeing it",
        "unit": "rate",
        "base_values": {"tier_1": 0.12, "tier_2": 0.15, "tier_3": 0.18},
        "business_logic": "click_count / impression_count. Higher in emerging markets due to offer novelty.",
        "derived_from": ["offer_impression_count"],
        "source_tables": ["play_loyalty.offer_clicks_v2"],
        "alert_threshold": "±10% WoW",
    },
    {
        "name": "offer_redemption_rate",
        "definition": "Percentage of users who completed offer redemption after clicking",
        "unit": "rate",
        "base_values": {"tier_1": 0.08, "tier_2": 0.11, "tier_3": 0.14},
        "business_logic": "redemption_count / click_count. Excludes expired offers.",
        "derived_from": ["offer_ctr"],
        "impacts": ["offer_driven_revenue", "offer_cost", "offer_roi"],
        "source_tables": ["play_loyalty.offer_redemptions_v3"],
        "alert_threshold": "±8% WoW",
    },
    {
        "name": "offer_redemption_count",
        "definition": "Absolute number of offer redemptions",
        "unit": "count",
        "base_values": {"tier_1": 12000, "tier_2": 25000, "tier_3": 8000},
        "source_tables": ["play_loyalty.offer_redemptions_v3"],
    },
    {
        "name": "offer_driven_revenue",
        "definition": "Total revenue from transactions where an offer was a factor",
        "unit": "usd",
        "base_values": {"tier_1": 850000, "tier_2": 400000, "tier_3": 120000},
        "business_logic": "Sum of transaction value where offer_id IS NOT NULL. Includes full transaction value, not just discounted portion.",
        "derived_from": ["offer_redemption_count"],
        "source_tables": ["play_commerce.transactions_v4", "play_loyalty.offer_redemptions_v3"],
        "caveats": "Attribution is last-touch. If user saw offer then purchased organically 3 days later, still attributed to offer.",
    },
    {
        "name": "offer_cost",
        "definition": "Total subsidy/discount cost of all redeemed offers",
        "unit": "usd",
        "base_values": {"tier_1": 180000, "tier_2": 95000, "tier_3": 30000},
        "source_tables": ["play_loyalty.offer_costs_v2"],
    },
    {
        "name": "offer_roi",
        "definition": "Return on offer investment: offer_driven_revenue / offer_cost",
        "unit": "ratio",
        "base_values": {"tier_1": 4.7, "tier_2": 4.2, "tier_3": 4.0},
        "derived_from": ["offer_driven_revenue", "offer_cost"],
        "alert_threshold": "Below 3.0 triggers review",
    },
    {
        "name": "avg_time_to_redemption",
        "definition": "Average hours from offer impression to redemption",
        "unit": "hours",
        "base_values": {"tier_1": 18.5, "tier_2": 12.3, "tier_3": 8.7},
        "business_logic": "Median would be more representative but we track mean for historical consistency.",
    },
    
    # === User Behavior ===
    {
        "name": "wau",
        "definition": "Weekly Active Users — unique users with at least 1 app open in trailing 7 days",
        "unit": "count",
        "base_values": {"tier_1": 45000000, "tier_2": 80000000, "tier_3": 25000000},
        "business_logic": "Excludes developer test accounts. Counted at user level, not device level.",
        "source_tables": ["play_core.user_activity_v5"],
        "alert_threshold": "±5% WoW",
        "impacts": ["revenue_per_user"],
    },
    {
        "name": "dau",
        "definition": "Daily Active Users — unique users with at least 1 app open per day",
        "unit": "count",
        "base_values": {"tier_1": 18000000, "tier_2": 35000000, "tier_3": 12000000},
        "source_tables": ["play_core.user_activity_v5"],
    },
    {
        "name": "d7_retention",
        "definition": "Percentage of users who return 7 days after their first activity in period",
        "unit": "rate",
        "base_values": {"tier_1": 0.32, "tier_2": 0.28, "tier_3": 0.22},
        "impacts": ["d30_retention", "wau"],
        "source_tables": ["play_core.retention_cohorts_v3"],
    },
    {
        "name": "d30_retention",
        "definition": "Percentage of users who return 30 days after first activity",
        "unit": "rate",
        "base_values": {"tier_1": 0.18, "tier_2": 0.14, "tier_3": 0.10},
        "source_tables": ["play_core.retention_cohorts_v3"],
    },
    {
        "name": "revenue_per_user",
        "definition": "Average revenue per active user in period (ARPU)",
        "unit": "usd",
        "base_values": {"tier_1": 4.50, "tier_2": 1.20, "tier_3": 0.45},
        "derived_from": ["wau"],
        "source_tables": ["play_commerce.transactions_v4", "play_core.user_activity_v5"],
    },
    {
        "name": "ltv_30d",
        "definition": "Projected 30-day lifetime value for users acquired in period",
        "unit": "usd",
        "base_values": {"tier_1": 8.20, "tier_2": 2.80, "tier_3": 0.95},
        "source_tables": ["play_ml.ltv_predictions_v2"],
        "caveats": "Model-predicted, not observed. Model retrained monthly.",
    },
    {
        "name": "sessions_per_user",
        "definition": "Average number of sessions per active user per week",
        "unit": "count",
        "base_values": {"tier_1": 4.2, "tier_2": 5.8, "tier_3": 6.5},
        "source_tables": ["play_core.session_data_v3"],
    },
    {
        "name": "avg_session_duration",
        "definition": "Average session length in minutes",
        "unit": "minutes",
        "base_values": {"tier_1": 12.5, "tier_2": 15.8, "tier_3": 18.2},
        "source_tables": ["play_core.session_data_v3"],
    },
    
    # === Loyalty Program ===
    {
        "name": "play_points_earn_rate",
        "definition": "Average Play Points earned per active member per week",
        "unit": "points",
        "base_values": {"tier_1": 45, "tier_2": 30, "tier_3": 20},
        "source_tables": ["play_loyalty.points_ledger_v2"],
    },
    {
        "name": "play_points_burn_rate",
        "definition": "Average Play Points redeemed per active member per week",
        "unit": "points",
        "base_values": {"tier_1": 28, "tier_2": 18, "tier_3": 12},
        "source_tables": ["play_loyalty.points_ledger_v2"],
    },
    {
        "name": "play_points_balance_avg",
        "definition": "Average outstanding Play Points balance per member",
        "unit": "points",
        "base_values": {"tier_1": 850, "tier_2": 520, "tier_3": 280},
        "source_tables": ["play_loyalty.points_ledger_v2"],
        "caveats": "High balances may indicate disengagement (not redeeming) rather than engagement.",
    },
    {
        "name": "loyalty_driven_purchases",
        "definition": "Purchases where Play Points earn or burn was a factor",
        "unit": "count",
        "base_values": {"tier_1": 35000, "tier_2": 55000, "tier_3": 15000},
        "source_tables": ["play_loyalty.points_ledger_v2", "play_commerce.transactions_v4"],
    },
    
    # === Funnel ===
    {
        "name": "store_visit_to_install_rate",
        "definition": "Conversion rate from Play Store page view to app install",
        "unit": "rate",
        "base_values": {"tier_1": 0.28, "tier_2": 0.32, "tier_3": 0.35},
        "source_tables": ["play_store.page_views_v3", "play_store.installs_v4"],
    },
    {
        "name": "install_to_first_purchase_rate",
        "definition": "Conversion from install to first in-app purchase within 7 days",
        "unit": "rate",
        "base_values": {"tier_1": 0.045, "tier_2": 0.032, "tier_3": 0.018},
        "source_tables": ["play_store.installs_v4", "play_commerce.transactions_v4"],
    },
    {
        "name": "offer_funnel_conversion",
        "definition": "End-to-end conversion: offer impression → click → redemption",
        "unit": "rate",
        "base_values": {"tier_1": 0.025, "tier_2": 0.035, "tier_3": 0.042},
        "derived_from": ["offer_ctr", "offer_redemption_rate"],
        "source_tables": ["play_loyalty.offer_funnel_v2"],
    },
]
```

#### 3.1.5 Initiatives to Generate (Minimum 15)

Generate a realistic calendar of Loyalty & Offers initiatives spanning the 18-month window. Each must have:

```python
INITIATIVE_SCHEMA = {
    "id": str,              # e.g. "INIT_001"
    "name": str,            # Human-readable campaign name
    "type": str,            # One of: PLAY_POINTS_BONUS, SUBSCRIPTION_TRIAL, DEVELOPER_PROMO,
                            # GIFT_CARD_BONUS, CASHBACK, RE_ENGAGEMENT, LOYALTY_TIER_UP, BUNDLE_OFFER
    "start_date": str,      # ISO date
    "end_date": str,        # ISO date
    "target_markets": list, # List of market_ids (or ["ALL"])
    "target_segments": list,# List of segment_ids (or ["ALL"])
    "target_categories": list, # List of category_ids (or ["ALL"])
    "impact": dict,         # metric_name → impact_magnitude (additive for counts, multiplicative for rates)
    "ramp_up_days": int,    # Days to reach full impact
    "decay_days": int,      # Days for impact to fade after end_date
    "experiment_id": str,   # Optional: linked experiment for ground truth
    "budget_usd": float,    # Total campaign budget
    "status": str,          # COMPLETED, ACTIVE, PLANNED
}
```

**Required initiative patterns** (these create the attribution challenges):

1. **Overlapping initiatives in same market** — 3-4 campaigns running concurrently in India during Diwali. Agent must disentangle.
2. **Global vs. local initiative overlap** — A global offer runs while a local market initiative is also active. Agent must separate effects.
3. **Initiative with negative side effects** — Win-back campaign boosts WAU but dilutes ARPU and lowers retention. Agent must capture the tradeoffs.
4. **Initiative that was paused mid-flight** — A campaign stopped early due to budget. Agent sees the start AND the abrupt stop as movements.
5. **Initiative with delayed impact** — Subscription trial has conversion 14-30 days after trial start. Agent must handle the lag.
6. **Null initiative** — A period where NO initiative was active but metrics moved due to seasonality alone. Agent must correctly attribute to seasonal, not hallucinate an initiative.

#### 3.1.6 Confounders to Generate (Minimum 5)

```python
CONFOUNDER_SCHEMA = {
    "name": str,
    "type": str,            # COMPETITOR_LAUNCH, POLICY_CHANGE, DATA_PIPELINE_ISSUE,
                            # ORGANIC_VIRAL, MACROECONOMIC, APP_STORE_ALGORITHM_CHANGE
    "date": str,
    "duration_days": int,
    "affected_markets": list,
    "affected_categories": list,
    "impact": dict,         # metric_name → impact
    "is_data_artifact": bool,  # True if this is a data quality issue, not a real market event
    "description": str,     # What happened (for the knowledge layer)
}
```

**Required confounders:**
1. A **data pipeline delay** that creates a 1-2 day phantom metric drop. The agent MUST learn to identify these as data issues, not real business changes.
2. A **competitor major launch** that impacts specific categories in specific markets.
3. A **platform policy change** that permanently shifts a metric baseline.
4. An **organic viral event** that spikes metrics temporarily with no associated initiative.
5. A **macroeconomic event** (e.g., currency devaluation in an emerging market) that affects revenue metrics.

#### 3.1.7 Data Generation Logic

The value for each cell in daily_metrics is computed as:

```
final_value = (base_value
              × market_modifier       # Different markets have different baselines
              × category_modifier     # Different categories have different baselines
              × segment_modifier      # Different segments have different baselines
              × structural_trend(date) # Long-term growth/decline
              × seasonality(date, market, category) # Periodic patterns
              + Σ initiative_impacts(date, market, category, segment, metric) # Campaign effects
              + Σ confounder_impacts(date, market, category, metric) # External events
              + noise(σ = base_value × 0.02)) # Random noise
```

Initiative impact uses a trapezoidal envelope:
```
impact_at_date = full_impact × envelope(date, start, end, ramp_up_days, decay_days)

where envelope =
  0                                    if date < start
  (date - start) / ramp_up_days       if date in [start, start + ramp_up]  (linear ramp)
  1.0                                  if date in [start + ramp_up, end]   (full impact)
  1 - (date - end) / decay_days       if date in [end, end + decay]       (linear decay)
  0                                    if date > end + decay
```

#### 3.1.8 Golden Dataset Generation

After generating daily_metrics, automatically detect metric movements:

```python
def detect_movements(daily_df, threshold_pct=0.05):
    """
    For each metric × market × category × segment:
    - Compute WoW change
    - If |change| > threshold, create a movement record
    - Attach ground truth: which initiatives and confounders were active
    - Compute ground truth attribution percentages
    """
```

Each golden record:
```python
GOLDEN_RECORD = {
    "movement_id": str,
    "date_detected": str,
    "metric_name": str,
    "market_id": str,
    "category_id": str,
    "segment_id": str,
    "magnitude_pct": float,
    "direction": str,  # "increase" or "decrease"
    "difficulty": str,  # EASY (single cause), MEDIUM (2-3 causes), HARD (4+ causes or confounders)
    
    # Ground truth (hidden from agent, used for eval)
    "ground_truth_attribution": [
        {"cause": "INIT_001 (Diwali 3x Points)", "contribution_pct": 0.55, "type": "initiative"},
        {"cause": "Diwali seasonal effect", "contribution_pct": 0.30, "type": "seasonal"},
        {"cause": "INIT_003 (Win-back)", "contribution_pct": 0.08, "type": "initiative"},
        {"cause": "Noise/organic", "contribution_pct": 0.07, "type": "residual"},
    ],
    "active_confounders": ["COMPETITOR_LAUNCH_US_GB"],  # Present but shouldn't affect this market
    "is_data_artifact": False,
    
    # Natural language ground truth (for LLM-as-judge reference)
    "ground_truth_narrative": "Offer redemption rate in India increased 22% WoW, primarily driven by the Diwali Play Points 3x Bonus campaign (55%) overlaid on organic Diwali seasonality (30%). A concurrent win-back campaign contributed marginally (8%). The competitor launch in US/GB markets had no impact on India."
}
```

**Target: 40-50 golden records** spanning:
- 10 EASY cases (single clear cause)
- 15 MEDIUM cases (2-3 concurrent causes)
- 10 HARD cases (4+ causes, confounders, or data artifacts)
- 5 EDGE cases (no initiative active, data pipeline issue, or contradictory signals)

---

### 3.2 AGENT PIPELINE (`src/agent/`)

#### 3.2.1 Pipeline Overview

The agent processes a natural language query through 6 deterministic + LLM stages:

```
Input: "Why did offer redemption rate increase 22% in India last week?"

→ Stage 1: QUERY PARSER [deterministic + light LLM]
  Extract: metric=offer_redemption_rate, market=IN, period=last_week, direction=increase, magnitude=22%

→ Stage 2: DATA FETCHER [deterministic — SQL on DuckDB]
  Pull: 
    - offer_redemption_rate for IN, all categories, all segments, last 4 weeks + YoY comparison
    - Adjacent metrics (offer_ctr, offer_driven_revenue, wau) for context
    - Same metric in other markets for comparison ("did this happen everywhere or just India?")

→ Stage 3: CONTEXT ENRICHER [deterministic — lookups]
  Gather:
    - Active initiatives during the period (from initiative_calendar)
    - Metric definition + business logic (from metric_definitions)
    - Seasonal baseline for this metric × market × period (from seasonal_patterns)
    - Pre-computed change points near this date (from change_points)
    - Confounder events near this date (from confounder_log)
    - Historical analyst RCA for similar movements (if available)

→ Stage 4: ATTRIBUTION REASONER [LLM — core reasoning step]
  Input: All data + context from stages 2-3
  Task: Generate structured attribution
  Output:
    {
      "movement_confirmed": true,
      "magnitude": "22.3% WoW increase",
      "hypotheses": [
        {"cause": "Diwali Play Points 3x Bonus", "type": "initiative", "estimated_contribution": 0.55, "confidence": "high", "evidence": "..."},
        {"cause": "Seasonal Diwali effect", "type": "seasonal", "estimated_contribution": 0.30, "confidence": "high", "evidence": "..."},
        ...
      ],
      "ruled_out": [
        {"cause": "Competitor launch", "reason": "Only affects US/GB markets, not India"}
      ],
      "data_quality_flags": [],
      "confidence_overall": "high"
    }

→ Stage 5: FACTUAL GROUNDING CHECK [deterministic + LLM]
  For each hypothesis:
    - Verify the cited initiative actually exists in the calendar
    - Verify the initiative targeted this market and segment
    - Verify the dates align
    - Verify the directional claim matches the data
    - Flag any claim not traceable to source data as [UNGROUNDED]

→ Stage 6: NARRATIVE GENERATOR [LLM]
  Input: Grounded attribution from stage 5
  Task: Generate analyst-quality report in the exec review format
  Output: Structured markdown report
```

#### 3.2.2 LLM Abstraction Layer

**CRITICAL FOR GCP PORTABILITY**: All LLM calls go through an abstraction:

```python
# src/agent/llm_client.py

from abc import ABC, abstractmethod

class LLMClient(ABC):
    @abstractmethod
    async def complete(self, system_prompt: str, user_prompt: str, 
                       temperature: float = 0.2, max_tokens: int = 4096) -> str:
        pass

class AnthropicClient(LLMClient):
    """Local development — uses Claude API"""
    async def complete(self, system_prompt, user_prompt, temperature=0.2, max_tokens=4096):
        # Anthropic API call
        pass

class VertexGeminiClient(LLMClient):
    """GCP deployment — uses Vertex AI Gemini API"""
    async def complete(self, system_prompt, user_prompt, temperature=0.2, max_tokens=4096):
        # Vertex AI Gemini API call
        pass

class LLMClientFactory:
    @staticmethod
    def create(provider: str = "anthropic") -> LLMClient:
        if provider == "anthropic":
            return AnthropicClient()
        elif provider == "vertex_gemini":
            return VertexGeminiClient()
        else:
            raise ValueError(f"Unknown provider: {provider}")
```

**Config-driven provider selection:**
```yaml
# config/local.yaml
llm:
  provider: anthropic
  model: claude-sonnet-4-20250514
  eval_model: claude-haiku-4-5-20251001

# config/gcp.yaml
llm:
  provider: vertex_gemini
  model: gemini-1.5-pro
  eval_model: gemini-1.5-flash
  project_id: your-gcp-project
  location: us-central1
```

#### 3.2.3 Data Fetcher Abstraction

**CRITICAL FOR GCP PORTABILITY**: Same pattern for data:

```python
# src/agent/data_client.py

class DataClient(ABC):
    @abstractmethod
    def query(self, sql: str) -> pd.DataFrame:
        pass

class DuckDBClient(DataClient):
    """Local — queries parquet files via DuckDB"""
    def __init__(self, data_dir: str):
        self.conn = duckdb.connect()
        self._register_tables(data_dir)
    
    def query(self, sql: str) -> pd.DataFrame:
        return self.conn.execute(sql).fetchdf()

class BigQueryClient(DataClient):
    """GCP — queries BigQuery tables"""
    def __init__(self, project_id: str, dataset: str):
        self.client = bigquery.Client(project=project_id)
        self.dataset = dataset
    
    def query(self, sql: str) -> pd.DataFrame:
        # Rewrite DuckDB SQL to BigQuery SQL if needed
        bq_sql = self._translate_sql(sql)
        return self.client.query(bq_sql).to_dataframe()
```

#### 3.2.4 Prompt Templates

Store all prompts in versioned template files, not inline code:

```
src/agent/prompts/
├── query_parser.txt          # Stage 1: Extract metric, market, period from natural language
├── attribution_reasoner.txt  # Stage 4: Core reasoning prompt
├── grounding_check.txt       # Stage 5: Verify claims against data
├── narrative_generator.txt   # Stage 6: Generate analyst-quality report
└── eval_judge.txt            # Eval: Score agent output against ground truth
```

**attribution_reasoner.txt** (the most important prompt):
```
You are a senior data analyst at Google Play's Loyalty & Offers team. You are investigating why a metric moved.

## METRIC MOVEMENT
{movement_summary}

## DATA
{fetched_data_tables}

## ACTIVE INITIATIVES DURING THIS PERIOD
{initiative_list}

## SEASONAL BASELINE
{seasonal_context}

## KNOWN EXTERNAL EVENTS
{confounder_list}

## METRIC DEFINITION & BUSINESS LOGIC
{metric_definition}

## ADJACENT METRIC MOVEMENTS
{adjacent_metrics}

## YOUR TASK
Analyze this metric movement and produce a structured attribution. Follow these rules:

1. LIST all plausible causes (initiatives, seasonality, external events, organic trends, data quality issues)
2. For each cause, ESTIMATE its percentage contribution to the total movement
3. Contributions must sum to approximately 100% (±10%)
4. For each estimate, cite the specific data that supports it
5. If data is insufficient to estimate a cause's contribution, say "insufficient data" — do NOT guess
6. Check if this is a DATA QUALITY ISSUE (pipeline delay, duplicate counting, etc.) before attributing to business causes
7. Explicitly note any active initiatives that you are RULING OUT as causes, and explain why

## OUTPUT FORMAT
Respond in this exact JSON structure:
{output_schema}
```

#### 3.2.5 Agent Output Schema

```python
AGENT_OUTPUT_SCHEMA = {
    "movement_summary": {
        "metric": str,
        "market": str,
        "period": str,
        "magnitude_pct": float,
        "direction": str,
    },
    "attribution": [
        {
            "cause": str,           # Name of the cause
            "type": str,            # initiative | seasonal | confounder | organic | data_quality
            "contribution_pct": float,  # 0.0 to 1.0
            "confidence": str,      # high | medium | low
            "evidence": str,        # Specific data supporting this claim
            "initiative_id": str,   # If type=initiative, the ID. Else null.
        }
    ],
    "ruled_out": [
        {
            "cause": str,
            "reason": str,
        }
    ],
    "data_quality_flags": [str],    # Any data quality concerns
    "overall_confidence": str,       # high | medium | low
    "recommendations": [str],        # Suggested next steps for the business team
    "narrative": str,                # Full analyst-quality report (generated in Stage 6)
}
```

---

### 3.3 EVAL ENGINE (`src/eval/`)

#### 3.3.1 Overview

The eval engine scores agent outputs against the golden dataset across 6 dimensions.

#### 3.3.2 Scoring Dimensions

```python
EVAL_DIMENSIONS = {
    "attribution_accuracy": {
        "description": "How close are the agent's contribution percentages to ground truth?",
        "scoring": "1 - mean_absolute_error(agent_contributions, ground_truth_contributions)",
        "weight": 0.30,
        "threshold_pass": 0.70,
    },
    "cause_identification": {
        "description": "Did the agent correctly identify all real causes?",
        "scoring": "F1 score of identified causes vs ground truth causes",
        "weight": 0.25,
        "threshold_pass": 0.80,
    },
    "false_attribution": {
        "description": "Did the agent attribute impact to non-existent causes?",
        "scoring": "1 - (false_attributions / total_attributions). Lower is worse.",
        "weight": 0.15,
        "threshold_pass": 0.90,
    },
    "data_artifact_detection": {
        "description": "For data quality issues, did the agent flag it correctly?",
        "scoring": "Binary: did agent identify data quality issue when is_data_artifact=True?",
        "weight": 0.10,
        "threshold_pass": 0.80,
    },
    "narrative_quality": {
        "description": "Is the narrative clear, actionable, and well-structured?",
        "scoring": "LLM-as-judge: 1-5 scale on clarity, actionability, structure, tone",
        "weight": 0.10,
        "threshold_pass": 3.5,
    },
    "factual_grounding": {
        "description": "Are all claims traceable to source data?",
        "scoring": "Proportion of claims with valid source citation",
        "weight": 0.10,
        "threshold_pass": 0.95,
    },
}
```

#### 3.3.3 Eval Runner

```python
# src/eval/runner.py

class EvalRunner:
    def __init__(self, agent_pipeline, golden_dataset, llm_judge):
        self.agent = agent_pipeline
        self.golden = golden_dataset
        self.judge = llm_judge
    
    async def run_full_eval(self) -> EvalReport:
        """Run agent on all golden cases and produce scorecard."""
        results = []
        
        for case in self.golden:
            # Run agent
            agent_output = await self.agent.process(case.query)
            
            # Score each dimension
            scores = {
                "attribution_accuracy": self._score_attribution(agent_output, case.ground_truth),
                "cause_identification": self._score_cause_id(agent_output, case.ground_truth),
                "false_attribution": self._score_false_attr(agent_output, case.ground_truth),
                "data_artifact_detection": self._score_data_artifact(agent_output, case),
                "narrative_quality": await self._score_narrative(agent_output, case),  # LLM judge
                "factual_grounding": self._score_grounding(agent_output),
            }
            
            # Log trace
            trace = self._log_trace(case, agent_output, scores)
            results.append({"case": case, "output": agent_output, "scores": scores, "trace": trace})
        
        return self._generate_report(results)
    
    def _generate_report(self, results) -> EvalReport:
        """Generate scorecard with aggregates, per-case details, and failure analysis."""
        # Aggregate scores
        # Identify worst failure modes
        # Group by difficulty level
        # Generate improvement suggestions
        pass
```

#### 3.3.4 LLM-as-Judge Prompt

```
You are evaluating the quality of an AI agent's metric attribution analysis.

## AGENT OUTPUT
{agent_narrative}

## GROUND TRUTH
{ground_truth_narrative}

## SCORE ON THESE DIMENSIONS (1-5 scale each):

1. CLARITY: Is the narrative easy to understand? Would a GM grasp the key message in 30 seconds?
2. ACTIONABILITY: Does it tell the business team what to do next?
3. COMPLETENESS: Did it cover all relevant factors, or miss important ones?
4. TONE: Is it appropriately analytical (not too hedged, not too confident)?
5. STRUCTURE: Does it follow the expected report format?

For each dimension, provide:
- Score (1-5)
- One-sentence justification

Output as JSON:
{"clarity": {"score": N, "reason": "..."}, ...}
```

#### 3.3.5 Eval Dashboard Data

The eval engine outputs:
- `eval_results.json` — per-case scores for all golden records
- `eval_summary.json` — aggregate scores, trends, failure analysis
- `eval_traces/` — full execution traces per case (for debugging)

---

### 3.4 OBSERVABILITY (`src/observability/`)

#### 3.4.1 Trace Schema

Every agent run produces a trace:

```python
TRACE_SCHEMA = {
    "trace_id": str,
    "timestamp": str,
    "query": str,
    "stages": [
        {
            "stage_name": str,          # "query_parser", "data_fetcher", etc.
            "start_time": float,
            "end_time": float,
            "duration_ms": float,
            "input_summary": str,       # Truncated input
            "output_summary": str,      # Truncated output
            "llm_calls": [              # Only for LLM stages
                {
                    "model": str,
                    "input_tokens": int,
                    "output_tokens": int,
                    "cost_usd": float,
                    "latency_ms": float,
                }
            ],
            "sql_queries": [str],       # Only for data fetcher
            "errors": [str],
        }
    ],
    "total_duration_ms": float,
    "total_cost_usd": float,
    "total_input_tokens": int,
    "total_output_tokens": int,
    "eval_scores": dict,                # If eval was run
}
```

#### 3.4.2 Local Implementation

```python
# src/observability/tracer.py

class Tracer:
    """JSON file-based tracer for local development. Swap for Cloud Trace on GCP."""
    
    def __init__(self, output_dir="./traces"):
        self.output_dir = output_dir
    
    def start_trace(self, query: str) -> TraceContext:
        pass
    
    def log_stage(self, ctx: TraceContext, stage_name: str, **kwargs):
        pass
    
    def end_trace(self, ctx: TraceContext) -> dict:
        # Write to JSON file
        pass
```

#### 3.4.3 GCP Equivalent

| Local | GCP | Notes |
|-------|-----|-------|
| JSON file tracer | Cloud Trace (OpenTelemetry) | Swap Tracer implementation, same interface |
| Per-stage timing | Cloud Trace spans | Each stage becomes a span |
| Cost tracking | Custom Cloud Monitoring metric | Push cost_usd to monitoring |
| Trace storage | BigQuery (for analysis) | Export traces to BigQuery table |
| Eval dashboard | Looker Studio or Vertex AI TensorBoard | Connect to BigQuery trace table |

---

### 3.5 WEB UI (`src/ui/`)

#### 3.5.1 Streamlit App — Pages

**Page 1: Attribution Agent**
- Text input: natural language query
- "Analyze" button
- Output panel showing:
  - Agent's attribution (pie chart of contributions)
  - Full narrative report
  - Confidence indicators per claim
  - Execution trace (collapsible: time per stage, tokens used, cost)
  - Data tables that were fetched (collapsible)

**Page 2: Eval Dashboard**
- Aggregate scorecard (overall accuracy, per-dimension scores)
- Score distribution by difficulty level (easy/medium/hard)
- Improvement trajectory chart (V1 → V2 → V3 if multiple versions run)
- Per-case detail table (click into any golden case to see agent output vs ground truth)
- Failure mode analysis (what types of cases does the agent struggle with?)

**Page 3: Data Explorer**
- Interactive exploration of the synthetic dataset
- Metric trend charts (select metric × market × category × segment)
- Initiative timeline overlay (see which campaigns were active when)
- Change point markers
- Journey archetype distribution over time
- This page is for the "under the hood" walkthrough — show how the data was constructed

**Page 4: System Architecture (Under the Hood)**
- Visual pipeline diagram (interactive: click each stage to see its prompt template, sample I/O)
- Prompt template viewer (show the actual prompts used at each stage)
- Config viewer (show what would change between local and GCP deployment)
- Data schema viewer (show all tables, columns, relationships)
- "How we built this" narrative walkthrough

#### 3.5.2 UI Design Notes

- Use Streamlit's dark theme (matches Google's internal tool aesthetics)
- Include LatentView branding subtly (logo in sidebar)
- Make every data element interactive (hover for details, click to expand)
- Show the "grounding" explicitly — every claim in the narrative should link to the data source

---

## 4. PROJECT STRUCTURE

```
play-attribution-pov/
│
├── README.md                          # Setup instructions, architecture overview
├── docker-compose.yaml                # Local development environment
├── Dockerfile                         # Container definition
├── Makefile                           # Common commands (generate, eval, serve, demo)
│
├── config/
│   ├── local.yaml                     # Local development config
│   ├── gcp.yaml                       # GCP deployment config
│   └── dimensions.yaml                # Markets, categories, segments, metrics
│
├── src/
│   ├── __init__.py
│   │
│   ├── data_generator/
│   │   ├── __init__.py
│   │   ├── generator.py               # Main generator class
│   │   ├── seasonality.py             # Seasonal pattern models
│   │   ├── initiatives.py             # Initiative definitions and impact model
│   │   ├── confounders.py             # External event definitions
│   │   ├── trends.py                  # Structural trend models
│   │   ├── golden_dataset.py          # Movement detection + ground truth generation
│   │   └── validate.py                # Sanity checks on generated data
│   │
│   ├── agent/
│   │   ├── __init__.py
│   │   ├── pipeline.py                # Main orchestrator (6-stage pipeline)
│   │   ├── query_parser.py            # Stage 1
│   │   ├── data_fetcher.py            # Stage 2
│   │   ├── context_enricher.py        # Stage 3
│   │   ├── attribution_reasoner.py    # Stage 4
│   │   ├── grounding_checker.py       # Stage 5
│   │   ├── narrative_generator.py     # Stage 6
│   │   ├── llm_client.py             # LLM abstraction (Anthropic / Vertex)
│   │   ├── data_client.py            # Data abstraction (DuckDB / BigQuery)
│   │   └── prompts/
│   │       ├── query_parser.txt
│   │       ├── attribution_reasoner.txt
│   │       ├── grounding_check.txt
│   │       ├── narrative_generator.txt
│   │       └── eval_judge.txt
│   │
│   ├── eval/
│   │   ├── __init__.py
│   │   ├── runner.py                  # Full eval pipeline
│   │   ├── scorers.py                 # Per-dimension scoring functions
│   │   ├── judge.py                   # LLM-as-judge implementation
│   │   ├── report.py                  # Scorecard and report generation
│   │   └── calibration.py            # Human vs LLM judge correlation
│   │
│   ├── observability/
│   │   ├── __init__.py
│   │   ├── tracer.py                  # Execution tracing
│   │   ├── cost_tracker.py            # Token/cost accounting
│   │   └── exporters/
│   │       ├── json_exporter.py       # Local: write to JSON files
│   │       └── cloud_trace_exporter.py # GCP: write to Cloud Trace
│   │
│   └── ui/
│       ├── app.py                     # Streamlit main app
│       ├── pages/
│       │   ├── 1_attribution_agent.py
│       │   ├── 2_eval_dashboard.py
│       │   ├── 3_data_explorer.py
│       │   └── 4_under_the_hood.py
│       └── components/
│           ├── charts.py              # Reusable chart components
│           ├── metric_card.py         # KPI display cards
│           └── trace_viewer.py        # Execution trace viewer
│
├── data/
│   ├── synthetic/                     # Generated synthetic data (gitignored)
│   │   ├── daily_metrics.parquet
│   │   ├── initiative_calendar.csv
│   │   ├── offer_catalog.csv
│   │   ├── metric_movements_golden.csv
│   │   ├── change_points.csv
│   │   ├── journey_aggregates.parquet
│   │   ├── metric_definitions.json
│   │   ├── seasonal_patterns.json
│   │   └── confounder_log.csv
│   └── eval/                          # Eval results (gitignored)
│       ├── eval_results.json
│       ├── eval_summary.json
│       └── traces/
│
├── scripts/
│   ├── generate_data.py               # CLI: generate synthetic data
│   ├── run_eval.py                    # CLI: run full eval suite
│   ├── demo.py                        # CLI: run a single demo query
│   └── deploy_gcp.sh                  # Script: deploy to GCP Cloud Run
│
├── tests/
│   ├── test_generator.py
│   ├── test_pipeline.py
│   ├── test_scorers.py
│   └── test_data_validation.py
│
├── notebooks/
│   ├── data_exploration.ipynb         # Explore generated data (for your team)
│   └── eval_analysis.ipynb            # Deep dive on eval results
│
├── docs/
│   ├── architecture.md                # Detailed architecture doc
│   ├── gcp_deployment.md              # GCP porting guide
│   ├── prompt_engineering.md          # Prompt design decisions
│   └── synthetic_data_design.md       # How/why the data was designed this way
│
└── requirements.txt                    # Python dependencies
```

---

## 5. DEPENDENCIES

```
# requirements.txt

# Core
python-dotenv==1.0.1
pyyaml==6.0.2
pydantic==2.10.0

# Data
duckdb==1.2.0
pandas==2.2.3
numpy==1.26.4
pyarrow==18.0.0
scipy==1.14.1

# LLM
anthropic==0.52.0         # Local development
google-cloud-aiplatform==1.75.0  # GCP deployment (optional for local)

# API
fastapi==0.115.0
uvicorn==0.34.0

# UI
streamlit==1.41.0
plotly==5.24.0
altair==5.4.0

# Eval
scikit-learn==1.5.2       # For F1 score, MAE calculations

# Observability
opentelemetry-api==1.29.0     # Optional, for GCP Cloud Trace export
opentelemetry-sdk==1.29.0

# Dev
pytest==8.3.0
pytest-asyncio==0.24.0
```

---

## 6. BUILD SEQUENCE (10-DAY PLAN)

### Day 1-2: Data Generator
**Deliverable**: `make generate` produces all synthetic data files

1. Implement `dimensions.yaml` with all markets, categories, segments, metrics
2. Implement `generator.py` with the value computation formula
3. Implement `seasonality.py` (weekly, monthly, quarterly, market-specific events)
4. Implement `initiatives.py` (15+ initiatives with trapezoidal impact envelopes)
5. Implement `confounders.py` (5+ external events including data pipeline delay)
6. Implement `trends.py` (structural trends with change points)
7. Implement `golden_dataset.py` (movement detection + ground truth attribution)
8. Run `validate.py` — plot 5-6 metrics, visually verify patterns look realistic

### Day 3-4: Agent Pipeline (V1)
**Deliverable**: `make demo QUERY="Why did offer redemption rate drop in India?"` produces output

1. Implement `llm_client.py` (Anthropic client only — GCP later)
2. Implement `data_client.py` (DuckDB client only — BigQuery later)
3. Implement `query_parser.py` + prompt template
4. Implement `data_fetcher.py` (SQL queries for relevant metrics, adjacent metrics, cross-market comparison)
5. Implement `context_enricher.py` (initiative lookup, metric definition lookup, seasonal baseline)
6. Implement `attribution_reasoner.py` + prompt template (THE key prompt — spend time here)
7. Implement `grounding_checker.py` (verify claims against data)
8. Implement `narrative_generator.py` + prompt template
9. Implement `pipeline.py` (orchestrate all 6 stages)
10. Test on 3-5 golden cases manually. Observe failures.

### Day 5-6: Eval Framework
**Deliverable**: `make eval` runs all golden cases and produces scorecard

1. Implement `scorers.py` (attribution accuracy, cause identification, false attribution, data artifact detection, grounding)
2. Implement `judge.py` (LLM-as-judge for narrative quality)
3. Implement `runner.py` (orchestrate eval across all golden cases)
4. Implement `report.py` (generate scorecard markdown + JSON)
5. Run first full eval. Get baseline scores.
6. Implement `tracer.py` and `cost_tracker.py` — instrument the agent pipeline

### Day 7-8: Iterate Agent (V2, V3)
**Deliverable**: Measurable improvement from V1 → V2 → V3

1. Analyze V1 eval failures. Identify top 3 failure modes.
2. Improve `attribution_reasoner.txt` prompt based on failures (most impact here)
3. Add pre-computed trend decomposition data to context enricher (reduces LLM reasoning burden)
4. Add the "adversarial self-check" step to grounding checker
5. Re-run eval → V2 scores
6. Further iterate on specific failure modes → V3 scores
7. Save V1, V2, V3 results for trajectory visualization

### Day 9-10: UI + Polish
**Deliverable**: `make serve` launches Streamlit app with all 4 pages working

1. Build Page 1: Attribution Agent (query input → output display)
2. Build Page 2: Eval Dashboard (scorecard, trajectory, per-case details)
3. Build Page 3: Data Explorer (metric trends, initiative timeline, journey archetypes)
4. Build Page 4: Under the Hood (architecture diagram, prompt viewer, schema viewer)
5. End-to-end testing: run 3 demo queries, verify full flow works
6. Polish: loading states, error handling, branding
7. Write README with setup instructions
8. Record a backup screen recording in case live demo fails in workshop

---

## 7. GCP DEPLOYMENT GUIDE

### 7.1 When to Port

Port AFTER the workshop, not before. The workshop demo runs locally. GCP deployment is for Phase 1 of the POC when the client has agreed to engage.

### 7.2 Porting Checklist

```
□ 1. Create GCP project (or use existing enterprise project)
□ 2. Enable APIs: Vertex AI, Cloud Run, BigQuery, Cloud Storage, Cloud Trace
□ 3. Upload synthetic data to GCS bucket
     gsutil cp -r data/synthetic/ gs://your-bucket/play-attribution/synthetic/
□ 4. Create BigQuery dataset and load parquet files
     bq load --source_format=PARQUET play_attribution.daily_metrics gs://your-bucket/.../daily_metrics.parquet
□ 5. Update config/gcp.yaml with project_id, dataset, bucket paths
□ 6. Switch LLM client: set provider=vertex_gemini in config
□ 7. Switch data client: set provider=bigquery in config
□ 8. Test agent pipeline with GCP backends
□ 9. Build Docker image and push to Artifact Registry
     docker build -t gcr.io/your-project/play-attribution .
     docker push gcr.io/your-project/play-attribution
□ 10. Deploy to Cloud Run
     gcloud run deploy play-attribution --image gcr.io/your-project/play-attribution
□ 11. Set up Cloud Trace export (swap tracer implementation)
□ 12. Set up Cloud Monitoring dashboard for cost/latency tracking
□ 13. Test end-to-end on GCP
□ 14. Share Cloud Run URL with client team
```

### 7.3 GCP Eval & Observability

**Vertex AI Evaluation Service:**
- Vertex AI has a native evaluation service for generative models
- It supports custom metrics, LLM-as-judge, and comparison across model versions
- In the GCP port, the eval runner can optionally push results to Vertex AI Eval alongside the custom eval pipeline
- This gives the Google team a familiar interface for viewing eval results

**Cloud Trace + Monitoring:**
- Replace JSON tracer with OpenTelemetry → Cloud Trace exporter
- Each agent stage becomes a Cloud Trace span
- Custom metrics (cost_per_query, attribution_accuracy) push to Cloud Monitoring
- Build a Cloud Monitoring dashboard that mirrors the Streamlit eval dashboard

**BigQuery for Trace Analysis:**
- Export all traces to a BigQuery table
- Enables SQL-based analysis of agent performance over time
- Connects to Looker Studio for dashboards the Google team is familiar with

### 7.4 What Changes Between Local and GCP (Summary)

| Aspect | Local | GCP | Code Change |
|--------|-------|-----|-------------|
| LLM calls | Claude API | Vertex AI Gemini | Change 1 config value |
| Data queries | DuckDB on parquet | BigQuery | Change 1 config value + minor SQL dialect differences |
| File storage | Local filesystem | GCS | Swap file paths to gs:// |
| Tracing | JSON files | Cloud Trace | Swap Tracer class |
| Cost tracking | In-memory counter | Cloud Monitoring | Add metrics exporter |
| UI hosting | localhost:8501 | Cloud Run | Docker deploy |
| Auth | None | IAM + service accounts | Add GCP auth middleware |
| Prompts | Same | Same | No change |
| Agent logic | Same | Same | No change |
| Eval logic | Same | Same | No change |

**Bottom line**: Agent pipeline code, prompt templates, eval scoring logic, and UI code are 100% identical between local and GCP. Only the infrastructure adapters change.

---

## 8. DEMO SCRIPT FOR WORKSHOP

### 8.1 Preparation Checklist

```
□ Laptop charged, power adapter packed
□ Streamlit app running locally, tested
□ 3 demo queries pre-selected (1 easy, 1 medium, 1 hard)
□ 1 failure case prepared (agent gets it wrong — intentional)
□ Backup: screen recording of full demo flow (in case of wifi/technical issues)
□ Backup: static screenshots of all 4 UI pages
□ Data explorer pre-loaded with interesting metric trends
□ Eval dashboard showing V1 → V2 → V3 trajectory
```

### 8.2 Demo Flow (15 minutes)

**Minute 0-2: Context**
"We built this in 10 days using synthetic data that mirrors your Loyalty & Offers domain. Let me show you what it does."

**Minute 2-5: Easy Query**
Run: "Why did Play Points burn rate spike 45% in India in late October?"
→ Agent correctly attributes to Diwali 3x Points Bonus
→ Show the attribution pie chart
→ Show the narrative output
→ Expand the trace: "Here's every step the agent took, the SQL it ran, the context it gathered, and the LLM cost"

**Minute 5-8: Hard Query**
Run: "What caused the offer redemption rate decline in US Casual Games in early November?"
→ Agent must disentangle: subscription trial ended (primary), competitor launch (secondary), seasonal post-Halloween dip (tertiary)
→ Show how it handles multiple concurrent causes
→ Point out the confidence levels differ per cause

**Minute 8-10: Failure Case**
Run: "Why did DAU drop 15% globally on November 8-9?"
→ Agent initially attributes to business causes
→ Show the data quality flag: "This was actually a pipeline delay — data arrived late, not a real user drop"
→ If agent catches it: "This is why the grounding check matters"
→ If agent misses it: "This is exactly why we need the eval framework — here's how we'd catch and fix this"

**Minute 10-12: Eval Dashboard**
→ Show the scorecard: "Across 40 test cases, here's how the agent performs"
→ Show the V1 → V3 improvement trajectory
→ Show failure mode analysis: "It struggles most on cases with 4+ concurrent initiatives — here's how we'd fix that in the POC"

**Minute 12-14: Under the Hood**
→ Show the architecture diagram: "6 deterministic stages with LLM reasoning only at decision points"
→ Show a prompt template: "This is the actual prompt — versioned, testable, improvable"
→ Show the data model: "Here's the synthetic data schema — when we get Plx access, we swap the data layer, everything else stays"

**Minute 14-15: The Transition**
"This entire system was built on synthetic data. When we start the POC and get Plx access, the transition takes about 2 weeks. The agent architecture, eval framework, prompt templates, and UI all transfer directly. You're not waiting for data access to see progress — we build ahead of it."

---

## 9. MAKE COMMANDS

```makefile
# Makefile

.PHONY: setup generate validate eval demo serve test clean

setup:
	pip install -r requirements.txt
	mkdir -p data/synthetic data/eval/traces

generate:
	python scripts/generate_data.py --output data/synthetic/ --months 18
	python src/data_generator/validate.py data/synthetic/

validate:
	python src/data_generator/validate.py data/synthetic/ --plots

eval:
	python scripts/run_eval.py --data data/synthetic/ --golden data/synthetic/metric_movements_golden.csv --output data/eval/

demo:
	python scripts/demo.py --query "$(QUERY)" --data data/synthetic/

serve:
	streamlit run src/ui/app.py -- --data-dir data/synthetic/ --eval-dir data/eval/

test:
	pytest tests/ -v

clean:
	rm -rf data/synthetic/* data/eval/*

# GCP commands
gcp-upload:
	gsutil cp -r data/synthetic/ gs://$(GCP_BUCKET)/play-attribution/synthetic/

gcp-build:
	docker build -t gcr.io/$(GCP_PROJECT)/play-attribution .
	docker push gcr.io/$(GCP_PROJECT)/play-attribution

gcp-deploy:
	bash scripts/deploy_gcp.sh
```

---

## 10. ACCEPTANCE CRITERIA

Before the workshop, the following must all pass:

| # | Criteria | How to Verify |
|---|---------|---------------|
| 1 | Synthetic data generates without errors | `make generate` completes |
| 2 | Data has visible seasonality, trends, and initiative impacts | `make validate` shows realistic plots |
| 3 | Golden dataset contains 40+ cases (10 easy, 15 medium, 10 hard, 5 edge) | Count records in golden CSV |
| 4 | Agent produces structured output for any valid query | Run 10 queries, all return valid JSON |
| 5 | Agent output includes attribution percentages that sum to ~100% | Automated check in pipeline |
| 6 | Eval scorecard is generated and shows per-dimension scores | `make eval` produces report |
| 7 | V1 baseline accuracy > 50% | Eval report shows score |
| 8 | At least one iteration improves accuracy (V2 > V1) | Compare eval reports |
| 9 | Streamlit UI loads with all 4 pages functional | Manual check |
| 10 | Demo query runs end-to-end in < 60 seconds | Timed |
| 11 | Execution trace shows per-stage timing and cost | Visible in UI |
| 12 | Data explorer shows interactive metric trends | Manual check |
| 13 | Under-the-hood page shows architecture and prompt templates | Manual check |
| 14 | Docker build succeeds | `docker build` completes |
| 15 | README has clear setup instructions | Follow from scratch on clean machine |
