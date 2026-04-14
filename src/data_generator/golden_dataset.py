"""Golden dataset generation — movement detection + ground truth attribution."""

import hashlib
import json
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from .initiatives import get_initiatives, _trapezoidal_envelope
from .confounders import get_confounders
from .seasonality import compute_seasonality


def detect_movements(daily_df: pd.DataFrame, threshold_pct: float = 0.05) -> pd.DataFrame:
    """Detect significant metric movements (WoW) and attach ground truth attribution.

    For each metric × market × category × segment, compute WoW change.
    If |change| > threshold, create a movement record with ground-truth labels.
    """
    df = daily_df.copy()
    df["date"] = pd.to_datetime(df["date"])

    # Aggregate to weekly averages
    df["week_start"] = df["date"] - pd.to_timedelta(df["date"].dt.dayofweek, unit="D")
    weekly = (
        df.groupby(["week_start", "market_id", "category_id", "segment_id", "metric_name"], observed=True)["value"]
        .mean()
        .reset_index()
    )
    weekly = weekly.sort_values(["market_id", "category_id", "segment_id", "metric_name", "week_start"])

    initiatives = get_initiatives()
    confounders = get_confounders()

    movements = []

    for (market, category, segment, metric), group in weekly.groupby(
        ["market_id", "category_id", "segment_id", "metric_name"], observed=True,
    ):
        if len(group) < 3:
            continue

        values = group["value"].values
        dates = group["week_start"].values

        for i in range(1, len(values)):
            prev = values[i - 1]
            curr = values[i]

            if abs(prev) < 1e-10:
                continue

            pct_change = (curr - prev) / abs(prev)

            if abs(pct_change) < threshold_pct:
                continue

            detection_date = pd.Timestamp(dates[i]).to_pydatetime()

            # Build ground truth attribution
            attribution = _build_ground_truth(
                detection_date, market, category, segment, metric,
                pct_change, initiatives, confounders,
            )

            if not attribution["causes"]:
                continue

            # Determine difficulty
            n_causes = len(attribution["causes"])
            has_confounder = any(c["type"] in ("confounder", "data_quality") for c in attribution["causes"])
            if n_causes == 1 and not has_confounder:
                difficulty = "EASY"
            elif n_causes <= 3 and not has_confounder:
                difficulty = "MEDIUM"
            elif has_confounder or n_causes >= 4:
                difficulty = "HARD"
            else:
                difficulty = "MEDIUM"

            movement_id = _make_id(detection_date, market, category, segment, metric)

            # Check if any active confounder is a data artifact
            is_artifact = any(c["type"] == "data_quality" for c in attribution["causes"])

            # Active confounders (even if they don't affect this specific combo)
            active_confs = [
                c["name"] for c in confounders
                if _confounder_active_on(detection_date, c)
            ]

            movements.append({
                "movement_id": movement_id,
                "date_detected": detection_date.strftime("%Y-%m-%d"),
                "metric_name": metric,
                "market_id": market,
                "category_id": category,
                "segment_id": segment,
                "magnitude_pct": round(pct_change * 100, 2),
                "direction": "increase" if pct_change > 0 else "decrease",
                "difficulty": difficulty,
                "ground_truth_attribution": json.dumps(attribution["causes"]),
                "active_confounders": json.dumps(active_confs),
                "is_data_artifact": is_artifact,
                "ground_truth_narrative": attribution["narrative"],
            })

    result = pd.DataFrame(movements)

    if result.empty:
        return pd.DataFrame(columns=[
            "movement_id", "date_detected", "metric_name", "market_id",
            "category_id", "segment_id", "magnitude_pct", "direction",
            "difficulty", "ground_truth_attribution", "active_confounders",
            "is_data_artifact", "ground_truth_narrative",
        ])

    # Sample to get target distribution: ~10 EASY, 15 MEDIUM, 10 HARD, 5 edge
    result = _sample_golden_set(result)
    return result


def _build_ground_truth(
    date: datetime, market: str, category: str, segment: str,
    metric: str, pct_change: float, initiatives: list, confounders: list,
) -> dict:
    """Determine which causes contributed to a movement and estimate shares."""
    causes = []
    total_impact = 0.0

    # Check initiatives
    for init in initiatives:
        if not _initiative_matches(date, market, category, segment, metric, init):
            continue

        impact_val = init["impact"].get(metric, 0)
        if impact_val == 0:
            continue

        start = datetime.fromisoformat(init["start_date"])
        end = datetime.fromisoformat(init["end_date"])
        envelope = _trapezoidal_envelope(date, start, end, init["ramp_up_days"], init["decay_days"])

        if envelope < 0.01:
            continue

        effective_impact = abs(impact_val * envelope)
        total_impact += effective_impact

        causes.append({
            "cause": f"{init['id']} ({init['name']})",
            "type": "initiative",
            "raw_impact": effective_impact,
            "initiative_id": init["id"],
        })

    # Check confounders
    for conf in confounders:
        if not _confounder_affects(date, market, category, metric, conf):
            continue

        impact_val = conf["impact"].get(metric, 0)
        if impact_val == 0:
            continue

        effective_impact = abs(impact_val)
        total_impact += effective_impact

        cause_type = "data_quality" if conf["is_data_artifact"] else "confounder"
        causes.append({
            "cause": conf["name"],
            "type": cause_type,
            "raw_impact": effective_impact,
        })

    # Check if seasonality could be a factor
    try:
        seasonal = compute_seasonality(date, market, category, metric)
        seasonal_effect = abs(seasonal - 1.0)
        if seasonal_effect > 0.05:
            total_impact += seasonal_effect * abs(pct_change)
            causes.append({
                "cause": "Seasonal pattern",
                "type": "seasonal",
                "raw_impact": seasonal_effect * abs(pct_change),
            })
    except Exception:
        pass

    # Add residual/organic
    if causes:
        organic_share = max(0.05, 1.0 - sum(c["raw_impact"] for c in causes) / (total_impact + 1e-10))
        causes.append({
            "cause": "Noise/organic",
            "type": "residual",
            "raw_impact": organic_share * total_impact if total_impact > 0 else 0.05,
        })

    # Normalize contributions to sum to ~100%
    if total_impact > 0:
        for c in causes:
            c["contribution_pct"] = round(c["raw_impact"] / (total_impact + causes[-1]["raw_impact"]), 2)
            del c["raw_impact"]
    elif causes:
        equal_share = round(1.0 / len(causes), 2)
        for c in causes:
            c["contribution_pct"] = equal_share
            if "raw_impact" in c:
                del c["raw_impact"]

    # Generate narrative
    narrative = _generate_narrative(date, market, category, segment, metric, pct_change, causes)

    return {"causes": causes, "narrative": narrative}


def _initiative_matches(
    date: datetime, market: str, category: str, segment: str,
    metric: str, init: dict,
) -> bool:
    """Check if an initiative is relevant to this cell at this date."""
    start = datetime.fromisoformat(init["start_date"])
    end = datetime.fromisoformat(init["end_date"])
    decay_end = end + timedelta(days=init["decay_days"])

    if date < start or date > decay_end:
        return False
    if "ALL" not in init["target_markets"] and market not in init["target_markets"]:
        return False
    if "ALL" not in init["target_categories"] and category not in init["target_categories"]:
        return False
    if "ALL" not in init["target_segments"] and segment not in init["target_segments"]:
        return False
    if metric not in init["impact"]:
        return False
    return True


def _confounder_affects(date: datetime, market: str, category: str, metric: str, conf: dict) -> bool:
    conf_start = datetime.fromisoformat(conf["date"])
    duration = min(conf["duration_days"], 365)
    conf_end = conf_start + timedelta(days=duration)

    if date < conf_start or date > conf_end:
        return False
    if "ALL" not in conf["affected_markets"] and market not in conf["affected_markets"]:
        return False
    if "ALL" not in conf["affected_categories"] and category not in conf["affected_categories"]:
        return False
    if metric not in conf["impact"]:
        return False
    return True


def _confounder_active_on(date: datetime, conf: dict) -> bool:
    conf_start = datetime.fromisoformat(conf["date"])
    duration = min(conf["duration_days"], 365)
    conf_end = conf_start + timedelta(days=duration)
    return conf_start <= date <= conf_end


def _generate_narrative(
    date: datetime, market: str, category: str, segment: str,
    metric: str, pct_change: float, causes: list,
) -> str:
    direction = "increased" if pct_change > 0 else "decreased"
    magnitude = abs(pct_change * 100)

    parts = [
        f"{metric.replace('_', ' ').title()} in {market} ({category}, {segment}) "
        f"{direction} {magnitude:.1f}% WoW around {date.strftime('%Y-%m-%d')}."
    ]

    sorted_causes = sorted(
        [c for c in causes if c["type"] != "residual"],
        key=lambda x: x.get("contribution_pct", 0),
        reverse=True,
    )

    if sorted_causes:
        primary = sorted_causes[0]
        parts.append(
            f"Primary driver: {primary['cause']} ({primary.get('contribution_pct', 0)*100:.0f}%)."
        )

    for c in sorted_causes[1:]:
        parts.append(
            f"Secondary factor: {c['cause']} ({c.get('contribution_pct', 0)*100:.0f}%)."
        )

    residual = [c for c in causes if c["type"] == "residual"]
    if residual:
        parts.append(f"Remaining {residual[0].get('contribution_pct', 0)*100:.0f}% attributed to noise/organic trends.")

    return " ".join(parts)


def _make_id(date: datetime, market: str, category: str, segment: str, metric: str) -> str:
    raw = f"{date.isoformat()}-{market}-{category}-{segment}-{metric}"
    return f"MOV_{hashlib.md5(raw.encode()).hexdigest()[:10].upper()}"


def _sample_golden_set(df: pd.DataFrame, target_total: int = 50) -> pd.DataFrame:
    """Sample golden records to get target distribution by difficulty."""
    targets = {"EASY": 12, "MEDIUM": 18, "HARD": 12}

    sampled = []
    for difficulty, n in targets.items():
        subset = df[df["difficulty"] == difficulty]
        if len(subset) >= n:
            sampled.append(subset.sample(n=n, random_state=42))
        else:
            sampled.append(subset)

    # Add edge cases — data artifacts and null-initiative periods
    edge_artifacts = df[df["is_data_artifact"] == True]
    edge_no_init = df[
        df["ground_truth_attribution"].str.contains('"type": "seasonal"')
        & ~df["ground_truth_attribution"].str.contains('"type": "initiative"')
    ]
    edge = pd.concat([edge_artifacts.head(3), edge_no_init.head(2)])
    sampled.append(edge)

    result = pd.concat(sampled).drop_duplicates(subset=["movement_id"]).head(target_total)
    return result.reset_index(drop=True)
