"""Full eval pipeline -- runs agent on all golden cases and produces scorecard."""

import json
import asyncio
import logging
from pathlib import Path
from datetime import datetime

import pandas as pd
import yaml

from .scorers import AttributionScorer
from .judge import LLMJudge
from .report import EvalReport
from ..agent.pipeline import AttributionPipeline
from ..agent.llm_client import LLMClientFactory

logger = logging.getLogger(__name__)


class EvalRunner:
    """Orchestrates evaluation: run agent on golden cases, score, report."""

    def __init__(self, config_path: str = "config/local.yaml"):
        self.pipeline = AttributionPipeline(config_path)

        with open(config_path) as f:
            config = yaml.safe_load(f)

        eval_llm = LLMClientFactory.create_eval_client(config["llm"])
        self.scorer = AttributionScorer()
        self.judge = LLMJudge(eval_llm)
        self.config = config

    # ── public API ───────────────────────────────────────────────────────

    async def run_full_eval(self, golden_path: str) -> dict:
        """Run agent on all golden cases and produce scorecard.

        Parameters
        ----------
        golden_path : str
            Path to ``metric_movements_golden.csv``.

        Returns
        -------
        dict
            Structured EvalReport (summary + per-case details).
        """
        golden_df = pd.read_csv(golden_path)
        results: list[dict] = []

        total = len(golden_df)
        for idx, (_, case) in enumerate(golden_df.iterrows(), 1):
            movement_id = case.get("movement_id", f"case_{idx}")
            logger.info("Evaluating case %s/%s: %s", idx, total, movement_id)
            print(f"[{idx}/{total}] Evaluating {movement_id}...", flush=True)

            try:
                result = await self._evaluate_single_case(case)
                results.append(result)
                wt = result.get("scores", {}).get("weighted_total", 0)
                print(f"  → score: {wt:.3f}", flush=True)
            except Exception:
                logger.exception("Failed to evaluate case %s", movement_id)
                results.append(self._error_result(case))
                print(f"  → ERROR (using zero scores)", flush=True)

            # Pace to stay under rate limits (30K tokens/min)
            if idx < total:
                await asyncio.sleep(15)

        return self._generate_report(results)

    async def run_single_eval(self, case: dict) -> dict:
        """Evaluate a single case dict (useful for debugging)."""
        case_series = pd.Series(case)
        return await self._evaluate_single_case(case_series)

    # ── internals ────────────────────────────────────────────────────────

    async def _evaluate_single_case(self, case: pd.Series) -> dict:
        """Score one golden case end-to-end."""
        query = self._construct_query(case)

        # Run the agent pipeline
        agent_output = await self.pipeline.process(query)

        # Parse ground truth attribution
        gt_attribution_raw = case.get("ground_truth_attribution", "[]")
        if isinstance(gt_attribution_raw, str):
            gt_attribution = json.loads(gt_attribution_raw)
        else:
            gt_attribution = gt_attribution_raw

        # Extract agent attribution list
        agent_attr_root = agent_output.get("attribution", {})
        if isinstance(agent_attr_root, dict):
            agent_attr = agent_attr_root.get("attribution", [])
        elif isinstance(agent_attr_root, list):
            agent_attr = agent_attr_root
        else:
            agent_attr = []

        # Deterministic scores
        is_artifact = bool(case.get("is_data_artifact", False))
        scores = {
            "attribution_accuracy": self.scorer.score_attribution_accuracy(
                agent_attr, gt_attribution
            ),
            "cause_identification": self.scorer.score_cause_identification(
                agent_attr, gt_attribution
            ),
            "false_attribution": self.scorer.score_false_attribution(
                agent_attr, gt_attribution
            ),
            "data_artifact_detection": self.scorer.score_data_artifact_detection(
                agent_output.get("attribution", {}), is_artifact
            ),
            "factual_grounding": self.scorer.score_factual_grounding(
                agent_output.get("grounding", {})
            ),
        }

        # LLM-as-judge for narrative quality
        gt_narrative = case.get("ground_truth_narrative", "")
        if pd.isna(gt_narrative):
            gt_narrative = ""
        narrative_scores = await self.judge.score_narrative(
            agent_output.get("narrative", ""),
            str(gt_narrative),
            agent_attr,
            gt_attribution,
        )
        scores["narrative_quality"] = narrative_scores.get("average_score", 0.5)

        # Weighted total
        scores["weighted_total"] = self.scorer.compute_weighted_score(scores)

        return {
            "movement_id": case.get("movement_id", "unknown"),
            "difficulty": case.get("difficulty", "UNKNOWN"),
            "metric_name": case.get("metric_name", "unknown"),
            "market_id": case.get("market_id", "unknown"),
            "scores": scores,
            "agent_output": agent_output,
            "narrative_scores": narrative_scores,
        }

    def _construct_query(self, case: pd.Series) -> str:
        """Construct a natural language query from a golden case."""
        direction = case["direction"]
        metric = str(case["metric_name"]).replace("_", " ")
        magnitude = abs(float(case["magnitude_pct"]))
        market = case["market_id"]
        date = case["date_detected"]
        return f"Why did {metric} {direction} {magnitude:.1f}% in {market} around {date}?"

    def _generate_report(self, results: list) -> dict:
        """Generate aggregate scorecard from individual results."""
        report = EvalReport(results)
        summary = report.generate_summary()

        # Persist artefacts if output dir is configured
        output_dir = self.config.get("eval", {}).get("output_dir")
        if output_dir:
            report.save(output_dir)

        return {
            "summary": summary,
            "results": results,
            "markdown": report.generate_markdown(),
        }

    @staticmethod
    def _error_result(case: pd.Series) -> dict:
        """Placeholder result for cases that errored during evaluation."""
        return {
            "movement_id": case.get("movement_id", "unknown"),
            "difficulty": case.get("difficulty", "UNKNOWN"),
            "metric_name": case.get("metric_name", "unknown"),
            "market_id": case.get("market_id", "unknown"),
            "scores": {
                "attribution_accuracy": 0.0,
                "cause_identification": 0.0,
                "false_attribution": 0.0,
                "data_artifact_detection": 0.0,
                "factual_grounding": 0.0,
                "narrative_quality": 0.0,
                "weighted_total": 0.0,
            },
            "agent_output": {},
            "narrative_scores": {"dimensions": {}, "average_score": 0.0},
            "error": True,
        }
