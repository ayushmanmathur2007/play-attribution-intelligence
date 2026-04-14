#!/usr/bin/env python3
"""CLI: Run full eval suite against golden dataset."""

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env", override=True)

from src.eval.runner import EvalRunner


async def run(args):
    print("=" * 60)
    print("ATTRIBUTION AGENT EVALUATION")
    print("=" * 60)
    print(f"Config: {args.config}")
    print(f"Golden dataset: {args.golden}")
    print(f"Output: {args.output}")
    print()

    runner = EvalRunner(config_path=args.config)
    report = await runner.run_full_eval(golden_path=args.golden)

    # Save results
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    import json
    with open(output_dir / "eval_results.json", "w") as f:
        json.dump(report.get("results", []), f, indent=2, default=str)

    with open(output_dir / "eval_summary.json", "w") as f:
        json.dump(report.get("summary", {}), f, indent=2, default=str)

    # Print summary
    summary = report.get("summary", {})
    print("\n" + "=" * 60)
    print("EVAL SUMMARY")
    print("=" * 60)
    print(f"Total cases: {summary.get('total_cases', 0)}")
    print(f"Overall weighted score: {summary.get('overall_weighted_score', 0):.3f}")
    print()

    for dim, score in summary.get("dimension_averages", {}).items():
        print(f"  {dim}: {score:.3f}")

    print()
    print(f"Results saved to: {args.output}")


def main():
    parser = argparse.ArgumentParser(description="Run attribution agent eval")
    parser.add_argument("--config", default="config/local.yaml", help="Config file path")
    parser.add_argument("--golden", default="data/synthetic/metric_movements_golden.csv",
                        help="Path to golden dataset CSV")
    parser.add_argument("--output", default="data/eval/", help="Output directory for results")
    args = parser.parse_args()

    asyncio.run(run(args))


if __name__ == "__main__":
    main()
