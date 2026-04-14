"""Scorecard and report generation from eval results."""

import json
from pathlib import Path
from datetime import datetime

import numpy as np

# Dimension display metadata
_DIMENSION_META = {
    "attribution_accuracy":     {"label": "Attribution Accuracy",     "weight": 0.30, "threshold": 0.70},
    "cause_identification":     {"label": "Cause Identification",     "weight": 0.25, "threshold": 0.80},
    "false_attribution":        {"label": "False Attribution",        "weight": 0.15, "threshold": 0.90},
    "data_artifact_detection":  {"label": "Data Artifact Detection",  "weight": 0.10, "threshold": 0.80},
    "narrative_quality":        {"label": "Narrative Quality",        "weight": 0.10, "threshold": 0.70},
    "factual_grounding":        {"label": "Factual Grounding",        "weight": 0.10, "threshold": 0.95},
}

_DIFFICULTIES = ("EASY", "MEDIUM", "HARD")


class EvalReport:
    """Build aggregate scorecard and reports from per-case eval results."""

    def __init__(self, results: list):
        self.results = results
        self.timestamp = datetime.utcnow().isoformat()

    # ── public API ───────────────────────────────────────────────────────

    def generate_summary(self) -> dict:
        """Generate aggregate summary.

        Returns
        -------
        dict
            {
                "timestamp": str,
                "total_cases": int,
                "overall_weighted_score": float,
                "per_dimension": {dim: {"mean", "median", "min", "max", "pass_rate"}},
                "per_difficulty": {diff: {"count", "mean_weighted", "per_dimension"}},
                "worst_cases": [...top 5...],
                "recommendations": [str, ...],
            }
        """
        if not self.results:
            return self._empty_summary()

        all_scores = [r["scores"] for r in self.results]

        # Overall weighted score
        weighted_totals = [s.get("weighted_total", 0.0) for s in all_scores]
        overall = float(np.mean(weighted_totals))

        # Per-dimension stats
        per_dimension: dict = {}
        for dim, meta in _DIMENSION_META.items():
            values = [s.get(dim, 0.0) for s in all_scores]
            arr = np.array(values)
            per_dimension[dim] = {
                "label": meta["label"],
                "weight": meta["weight"],
                "threshold": meta["threshold"],
                "mean": round(float(np.mean(arr)), 4),
                "median": round(float(np.median(arr)), 4),
                "min": round(float(np.min(arr)), 4),
                "max": round(float(np.max(arr)), 4),
                "pass_rate": round(float(np.mean(arr >= meta["threshold"])), 4),
            }

        # Per-difficulty breakdown
        per_difficulty: dict = {}
        for diff in _DIFFICULTIES:
            subset = [r for r in self.results if r.get("difficulty", "UNKNOWN").upper() == diff]
            if not subset:
                per_difficulty[diff] = {"count": 0, "mean_weighted": None, "per_dimension": {}}
                continue
            sub_scores = [r["scores"] for r in subset]
            sub_weighted = [s.get("weighted_total", 0.0) for s in sub_scores]
            dim_means = {}
            for dim in _DIMENSION_META:
                vals = [s.get(dim, 0.0) for s in sub_scores]
                dim_means[dim] = round(float(np.mean(vals)), 4)
            per_difficulty[diff] = {
                "count": len(subset),
                "mean_weighted": round(float(np.mean(sub_weighted)), 4),
                "per_dimension": dim_means,
            }

        # Handle UNKNOWN difficulty cases
        unknown_subset = [
            r for r in self.results
            if r.get("difficulty", "UNKNOWN").upper() not in _DIFFICULTIES
        ]
        if unknown_subset:
            sub_scores = [r["scores"] for r in unknown_subset]
            sub_weighted = [s.get("weighted_total", 0.0) for s in sub_scores]
            dim_means = {}
            for dim in _DIMENSION_META:
                vals = [s.get(dim, 0.0) for s in sub_scores]
                dim_means[dim] = round(float(np.mean(vals)), 4)
            per_difficulty["UNKNOWN"] = {
                "count": len(unknown_subset),
                "mean_weighted": round(float(np.mean(sub_weighted)), 4),
                "per_dimension": dim_means,
            }

        # Top 5 worst cases
        sorted_results = sorted(self.results, key=lambda r: r["scores"].get("weighted_total", 0.0))
        worst_cases = []
        for r in sorted_results[:5]:
            failing_dims = [
                dim for dim, meta in _DIMENSION_META.items()
                if r["scores"].get(dim, 0.0) < meta["threshold"]
            ]
            worst_cases.append({
                "movement_id": r.get("movement_id"),
                "difficulty": r.get("difficulty"),
                "metric_name": r.get("metric_name"),
                "market_id": r.get("market_id"),
                "weighted_total": r["scores"].get("weighted_total", 0.0),
                "failing_dimensions": failing_dims,
            })

        # Recommendations
        recommendations = self._generate_recommendations(per_dimension, per_difficulty)

        return {
            "timestamp": self.timestamp,
            "total_cases": len(self.results),
            "overall_weighted_score": round(overall, 4),
            "per_dimension": per_dimension,
            "per_difficulty": per_difficulty,
            "worst_cases": worst_cases,
            "recommendations": recommendations,
        }

    def generate_markdown(self) -> str:
        """Generate a markdown scorecard report."""
        summary = self.generate_summary()
        lines: list[str] = []

        lines.append("# Eval Scorecard")
        lines.append("")
        lines.append(f"**Generated:** {self.timestamp}")
        lines.append(f"**Total cases:** {summary['total_cases']}")
        lines.append(f"**Overall weighted score:** {summary['overall_weighted_score']:.4f}")
        lines.append("")

        # Pass/fail verdict
        passing = summary["overall_weighted_score"] >= 0.70
        lines.append(f"**Verdict:** {'PASS' if passing else 'FAIL'}")
        lines.append("")

        # Per-dimension table
        lines.append("## Per-Dimension Scores")
        lines.append("")
        lines.append("| Dimension | Weight | Threshold | Mean | Median | Min | Max | Pass Rate |")
        lines.append("|-----------|--------|-----------|------|--------|-----|-----|-----------|")
        for dim, stats in summary["per_dimension"].items():
            status = "+" if stats["mean"] >= stats["threshold"] else "-"
            lines.append(
                f"| {status} {stats['label']} | {stats['weight']:.2f} | "
                f"{stats['threshold']:.2f} | {stats['mean']:.4f} | {stats['median']:.4f} | "
                f"{stats['min']:.4f} | {stats['max']:.4f} | {stats['pass_rate']:.0%} |"
            )
        lines.append("")

        # Per-difficulty breakdown
        lines.append("## Per-Difficulty Breakdown")
        lines.append("")
        for diff, data in summary["per_difficulty"].items():
            if data["count"] == 0:
                continue
            lines.append(f"### {diff} (n={data['count']})")
            lines.append(f"- Mean weighted score: {data['mean_weighted']:.4f}")
            for dim, val in data["per_dimension"].items():
                label = _DIMENSION_META.get(dim, {}).get("label", dim)
                lines.append(f"  - {label}: {val:.4f}")
            lines.append("")

        # Worst cases
        lines.append("## Top 5 Worst Cases")
        lines.append("")
        if summary["worst_cases"]:
            lines.append("| # | Movement ID | Difficulty | Metric | Market | Score | Failing Dimensions |")
            lines.append("|---|-------------|------------|--------|--------|-------|--------------------|")
            for i, wc in enumerate(summary["worst_cases"], 1):
                failing = ", ".join(wc["failing_dimensions"]) or "none"
                lines.append(
                    f"| {i} | {wc['movement_id']} | {wc['difficulty']} | "
                    f"{wc['metric_name']} | {wc['market_id']} | "
                    f"{wc['weighted_total']:.4f} | {failing} |"
                )
            lines.append("")
        else:
            lines.append("No cases to report.")
            lines.append("")

        # Recommendations
        lines.append("## Recommendations")
        lines.append("")
        for rec in summary.get("recommendations", []):
            lines.append(f"- {rec}")
        lines.append("")

        return "\n".join(lines)

    def save(self, output_dir: str):
        """Save eval_results.json and eval_summary.json to output directory."""
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)

        # Serialisable results (strip non-JSON-safe agent_output)
        safe_results = []
        for r in self.results:
            entry = {k: v for k, v in r.items() if k != "agent_output"}
            # Keep a minimal copy of agent_output (just attribution + cost)
            ao = r.get("agent_output", {})
            entry["agent_output_summary"] = {
                "has_attribution": bool(ao.get("attribution")),
                "has_grounding": bool(ao.get("grounding")),
                "has_narrative": bool(ao.get("narrative")),
                "cost": ao.get("cost"),
            }
            safe_results.append(entry)

        results_path = out / "eval_results.json"
        results_path.write_text(json.dumps(safe_results, indent=2, default=str))

        summary = self.generate_summary()
        summary_path = out / "eval_summary.json"
        summary_path.write_text(json.dumps(summary, indent=2, default=str))

        md_path = out / "eval_scorecard.md"
        md_path.write_text(self.generate_markdown())

    # ── internal helpers ─────────────────────────────────────────────────

    def _empty_summary(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "total_cases": 0,
            "overall_weighted_score": 0.0,
            "per_dimension": {},
            "per_difficulty": {},
            "worst_cases": [],
            "recommendations": ["No cases evaluated. Check golden dataset path."],
        }

    @staticmethod
    def _generate_recommendations(per_dimension: dict, per_difficulty: dict) -> list[str]:
        """Infer actionable recommendations from failure patterns."""
        recs: list[str] = []

        # Flag any dimension below threshold
        for dim, stats in per_dimension.items():
            if stats["mean"] < stats["threshold"]:
                gap = stats["threshold"] - stats["mean"]
                recs.append(
                    f"{stats['label']} is {gap:.2f} below threshold ({stats['mean']:.2f} vs "
                    f"{stats['threshold']:.2f}). "
                    f"Pass rate is only {stats['pass_rate']:.0%}."
                )

        # Check for difficulty-related degradation
        easy_score = (per_difficulty.get("EASY") or {}).get("mean_weighted")
        hard_score = (per_difficulty.get("HARD") or {}).get("mean_weighted")
        if easy_score is not None and hard_score is not None and easy_score - hard_score > 0.15:
            recs.append(
                f"Significant performance drop on HARD cases ({hard_score:.2f}) vs "
                f"EASY ({easy_score:.2f}). Consider improving multi-cause reasoning."
            )

        # Specific dimension advice
        attr_acc = per_dimension.get("attribution_accuracy", {})
        if attr_acc.get("mean", 1.0) < attr_acc.get("threshold", 0.7):
            recs.append(
                "Attribution percentages are inaccurate. Review the attribution reasoner prompt "
                "and consider adding few-shot examples with percentage breakdowns."
            )

        false_attr = per_dimension.get("false_attribution", {})
        if false_attr.get("mean", 1.0) < false_attr.get("threshold", 0.9):
            recs.append(
                "Agent is hallucinating causes. Add explicit instruction to only attribute "
                "causes supported by data evidence."
            )

        data_art = per_dimension.get("data_artifact_detection", {})
        if data_art.get("mean", 1.0) < data_art.get("threshold", 0.8):
            recs.append(
                "Data artifact detection is weak. Add a dedicated data-quality check step "
                "before attribution reasoning."
            )

        grounding = per_dimension.get("factual_grounding", {})
        if grounding.get("mean", 1.0) < grounding.get("threshold", 0.95):
            recs.append(
                "Factual grounding is below threshold. Tighten the grounding checker to "
                "require explicit source references for every claim."
            )

        narrative = per_dimension.get("narrative_quality", {})
        if narrative.get("mean", 1.0) < narrative.get("threshold", 0.7):
            recs.append(
                "Narrative quality is low. Review the narrative generator prompt for clarity "
                "and actionability improvements."
            )

        if not recs:
            recs.append("All dimensions meet thresholds. Consider raising thresholds or adding harder test cases.")

        return recs
