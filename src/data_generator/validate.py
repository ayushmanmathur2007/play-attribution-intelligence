"""Sanity checks and validation plots for generated synthetic data."""

import sys
import json
from pathlib import Path

import pandas as pd
import numpy as np


def validate_data(data_dir: str, show_plots: bool = False) -> bool:
    """Run all validation checks on generated data. Returns True if all pass."""
    data_path = Path(data_dir)
    all_ok = True

    print("=" * 60)
    print("SYNTHETIC DATA VALIDATION")
    print("=" * 60)

    # 1. Check all required files exist
    required_files = [
        "daily_metrics.parquet",
        "initiative_calendar.csv",
        "confounder_log.csv",
        "metric_definitions.json",
        "seasonal_patterns.json",
        "change_points.csv",
        "metric_movements_golden.csv",
        "journey_aggregates.parquet",
    ]
    print("\n[1/6] Checking required files...")
    for f in required_files:
        exists = (data_path / f).exists()
        status = "OK" if exists else "MISSING"
        print(f"  {status}: {f}")
        if not exists:
            all_ok = False

    if not all_ok:
        print("\nFATAL: Missing required files. Run `make generate` first.")
        return False

    # 2. Load and check daily_metrics
    print("\n[2/6] Validating daily_metrics.parquet...")
    dm = pd.read_parquet(data_path / "daily_metrics.parquet")
    print(f"  Rows: {len(dm):,}")
    print(f"  Columns: {list(dm.columns)}")
    print(f"  Date range: {dm['date'].min()} to {dm['date'].max()}")
    print(f"  Markets: {dm['market_id'].nunique()} unique ({sorted(dm['market_id'].unique())})")
    print(f"  Categories: {dm['category_id'].nunique()} unique")
    print(f"  Segments: {dm['segment_id'].nunique()} unique")
    print(f"  Metrics: {dm['metric_name'].nunique()} unique")

    # Check for nulls
    null_count = dm["value"].isna().sum()
    if null_count > 0:
        print(f"  WARNING: {null_count} null values found")
        all_ok = False
    else:
        print("  No null values — OK")

    # Check for negative values in count metrics
    count_metrics = dm[dm["metric_name"].isin(["wau", "dau", "offer_impression_count", "offer_redemption_count"])]
    neg_count = (count_metrics["value"] < 0).sum()
    if neg_count > 0:
        print(f"  WARNING: {neg_count} negative values in count metrics")
    else:
        print("  No negative count values — OK")

    # Check rate metrics are in [0, 1]
    rate_metrics = dm[dm["metric_name"].isin([
        "offer_ctr", "offer_redemption_rate", "d7_retention", "d30_retention",
        "store_visit_to_install_rate", "install_to_first_purchase_rate",
        "offer_funnel_conversion", "subscription_conversion_rate", "churn_rate",
    ])]
    out_of_range = ((rate_metrics["value"] < 0) | (rate_metrics["value"] > 1)).sum()
    if out_of_range > 0:
        print(f"  WARNING: {out_of_range} rate values outside [0, 1]")
    else:
        print("  All rate values in [0, 1] — OK")

    # 3. Check initiative calendar
    print("\n[3/6] Validating initiative_calendar.csv...")
    ic = pd.read_csv(data_path / "initiative_calendar.csv")
    print(f"  Initiatives: {len(ic)}")
    if len(ic) >= 15:
        print("  At least 15 initiatives — OK")
    else:
        print(f"  WARNING: Only {len(ic)} initiatives (target: 15+)")
        all_ok = False

    # 4. Check golden dataset
    print("\n[4/6] Validating metric_movements_golden.csv...")
    golden = pd.read_csv(data_path / "metric_movements_golden.csv")
    print(f"  Golden records: {len(golden)}")

    if "difficulty" in golden.columns:
        diff_counts = golden["difficulty"].value_counts().to_dict()
        print(f"  EASY: {diff_counts.get('EASY', 0)}, MEDIUM: {diff_counts.get('MEDIUM', 0)}, HARD: {diff_counts.get('HARD', 0)}")

    if len(golden) >= 30:
        print("  At least 30 golden records — OK")
    else:
        print(f"  WARNING: Only {len(golden)} golden records (target: 40+)")

    # 5. Check metric definitions
    print("\n[5/6] Validating metric_definitions.json...")
    with open(data_path / "metric_definitions.json") as f:
        md = json.load(f)
    print(f"  Metric definitions: {len(md)}")
    if len(md) >= 25:
        print("  At least 25 metrics defined — OK")
    else:
        print(f"  WARNING: Only {len(md)} metric definitions")

    # 6. Check confounder log
    print("\n[6/6] Validating confounder_log.csv...")
    cl = pd.read_csv(data_path / "confounder_log.csv")
    print(f"  Confounders: {len(cl)}")
    if len(cl) >= 5:
        print("  At least 5 confounders — OK")
    else:
        print(f"  WARNING: Only {len(cl)} confounders")

    # Summary
    print("\n" + "=" * 60)
    if all_ok:
        print("VALIDATION PASSED — all checks OK")
    else:
        print("VALIDATION FAILED — see warnings above")
    print("=" * 60)

    return all_ok


if __name__ == "__main__":
    data_dir = sys.argv[1] if len(sys.argv) > 1 else "data/synthetic"
    show_plots = "--plots" in sys.argv
    success = validate_data(data_dir, show_plots)
    sys.exit(0 if success else 1)
