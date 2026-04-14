#!/usr/bin/env python3
"""CLI: Run a single demo query through the attribution pipeline."""

import argparse
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from src.agent.pipeline import AttributionPipeline


EXAMPLE_QUERIES = [
    "Why did offer redemption rate increase 22% in India in late October 2024?",
    "What caused Play Points burn rate to spike 45% in India in late October?",
    "Why did DAU drop 15% globally on November 8-9 2024?",
    "What drove offer driven revenue decline in US Casual Games in early November?",
    "Why did store visit to install rate jump in the US in late August 2024?",
]


async def run_demo(query: str, config: str):
    print("=" * 70)
    print("PLAY ATTRIBUTION INTELLIGENCE — DEMO")
    print("=" * 70)
    print(f"\nQuery: {query}")
    print("-" * 70)

    pipeline = AttributionPipeline(config_path=config)
    result = await pipeline.process(query)

    if "error" in result:
        print(f"\nERROR: {result['error']}")
        return

    # Print narrative
    print("\n## NARRATIVE REPORT")
    print(result.get("narrative", "No narrative generated."))

    # Print attribution summary
    attribution = result.get("attribution", {})
    if "attribution" in attribution:
        print("\n## ATTRIBUTION BREAKDOWN")
        for attr in attribution["attribution"]:
            pct = attr.get("contribution_pct", 0)
            conf = attr.get("confidence", "?")
            print(f"  [{conf.upper():>6}] {pct*100:5.1f}%  {attr.get('cause', 'Unknown')}")

    # Print grounding
    grounding = result.get("grounding", {})
    if grounding:
        print(f"\n## GROUNDING SCORE: {grounding.get('grounding_score', 'N/A')}")

    # Print cost/performance
    cost = result.get("cost", {})
    trace = result.get("trace", {})
    print(f"\n## PERFORMANCE")
    print(f"  Total cost: ${cost.get('total_cost_usd', 0):.4f}")
    print(f"  Input tokens: {cost.get('total_input_tokens', 0):,}")
    print(f"  Output tokens: {cost.get('total_output_tokens', 0):,}")
    print(f"  Total duration: {trace.get('total_duration_ms', 0)/1000:.1f}s")
    print(f"  LLM calls: {cost.get('num_calls', 0)}")

    # Stage breakdown
    if "stages" in trace:
        print(f"\n## STAGE TIMING")
        for stage in trace["stages"]:
            print(f"  {stage['stage_name']:25s} {stage['duration_ms']:8.0f}ms")

    print("\n" + "=" * 70)


def main():
    parser = argparse.ArgumentParser(description="Run a demo attribution query")
    parser.add_argument("--query", "-q", type=str, help="The query to analyze")
    parser.add_argument("--config", default="config/local.yaml", help="Config file")
    parser.add_argument("--list-examples", action="store_true", help="List example queries")
    parser.add_argument("--example", "-e", type=int, help="Run example query N (1-indexed)")
    args = parser.parse_args()

    if args.list_examples:
        print("Example queries:")
        for i, q in enumerate(EXAMPLE_QUERIES, 1):
            print(f"  {i}. {q}")
        return

    if args.example:
        if 1 <= args.example <= len(EXAMPLE_QUERIES):
            query = EXAMPLE_QUERIES[args.example - 1]
        else:
            print(f"Example {args.example} not found. Use --list-examples to see available.")
            return
    elif args.query:
        query = args.query
    else:
        print("Provide --query or --example. Use --list-examples to see examples.")
        return

    asyncio.run(run_demo(query, args.config))


if __name__ == "__main__":
    main()
