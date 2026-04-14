"""Per-dimension scoring functions for attribution evaluation."""

import json
import numpy as np
from sklearn.metrics import f1_score, mean_absolute_error


class AttributionScorer:
    """Score agent output against golden ground truth across 6 dimensions."""

    # ── helpers ──────────────────────────────────────────────────────────

    @staticmethod
    def _normalize_cause_key(cause: dict) -> str:
        """Build a canonical key for matching causes between agent and ground truth.

        For initiative-type causes the key is   "<type>:<initiative_id>".
        For everything else (seasonal, confounder, etc.) the key is "<type>".
        """
        ctype = (cause.get("type", cause.get("cause_type", "unknown")) or "unknown").lower().strip()
        init_id = (cause.get("initiative_id", cause.get("id", "")) or "").strip()
        if init_id:
            return f"{ctype}:{init_id}"
        return ctype

    @staticmethod
    def _fuzzy_match_key(agent_cause: dict, gt_keys: set[str]) -> str | None:
        """Try to match an agent cause to a ground-truth key.

        Exact key match first, then fall back to substring checks:
        - Does any gt key's initiative_id appear inside the agent cause name/description?
        - Does the agent cause share the same type with any unmatched gt key?
        """
        agent_key = AttributionScorer._normalize_cause_key(agent_cause)
        if agent_key in gt_keys:
            return agent_key

        # Substring: check if any gt initiative_id appears in agent cause text
        agent_text = json.dumps(agent_cause).lower()
        for gt_key in gt_keys:
            if ":" in gt_key:
                init_id = gt_key.split(":", 1)[1]
                if init_id and init_id.lower() in agent_text:
                    return gt_key

        # Same type fallback (only for non-initiative types)
        agent_type = (agent_cause.get("type", agent_cause.get("cause_type", "")) or "").lower().strip()
        for gt_key in gt_keys:
            if ":" not in gt_key and gt_key == agent_type:
                return gt_key

        return None

    # ── dimension scorers ────────────────────────────────────────────────

    def score_attribution_accuracy(
        self,
        agent_attribution: list[dict],
        ground_truth: list[dict],
    ) -> float:
        """How close are contribution percentages to ground truth?

        Score = 1 - MAE(agent_contributions, ground_truth_contributions)
        Align causes by matching agent cause names to ground truth cause names
        (fuzzy match on initiative IDs).  For unmatched causes, treat the
        agent's contribution as error.

        Weight: 0.30, threshold: 0.70
        """
        if not ground_truth:
            return 1.0 if not agent_attribution else 0.0

        gt_map: dict[str, float] = {}
        for cause in ground_truth:
            key = self._normalize_cause_key(cause)
            gt_map[key] = float(cause.get("contribution_pct", cause.get("contribution", 0)))

        remaining_gt_keys = set(gt_map.keys())
        agent_values: list[float] = []
        gt_values: list[float] = []

        for agent_cause in agent_attribution:
            agent_pct = float(
                agent_cause.get("contribution_pct", agent_cause.get("contribution", 0))
            )
            matched_key = self._fuzzy_match_key(agent_cause, remaining_gt_keys)
            if matched_key is not None:
                agent_values.append(agent_pct)
                gt_values.append(gt_map[matched_key])
                remaining_gt_keys.discard(matched_key)
            else:
                # Unmatched agent cause — pure error
                agent_values.append(agent_pct)
                gt_values.append(0.0)

        # Any unmatched ground-truth causes the agent missed entirely
        for key in remaining_gt_keys:
            agent_values.append(0.0)
            gt_values.append(gt_map[key])

        # Normalise to 0-1 range (percentages are 0-100)
        agent_arr = np.array(agent_values) / 100.0
        gt_arr = np.array(gt_values) / 100.0

        mae = mean_absolute_error(gt_arr, agent_arr)
        score = max(0.0, 1.0 - mae)
        return round(score, 4)

    def score_cause_identification(
        self,
        agent_attribution: list[dict],
        ground_truth: list[dict],
    ) -> float:
        """F1 score of identified causes vs ground truth causes.

        Match causes by type + initiative_id (for initiatives) or by type
        (for seasonal/confounder).

        Weight: 0.25, threshold: 0.80
        """
        if not ground_truth and not agent_attribution:
            return 1.0
        if not ground_truth or not agent_attribution:
            return 0.0

        gt_keys = {self._normalize_cause_key(c) for c in ground_truth}
        all_keys = list(gt_keys)

        # Determine which gt keys the agent found
        matched_gt: set[str] = set()
        for agent_cause in agent_attribution:
            matched = self._fuzzy_match_key(agent_cause, gt_keys - matched_gt)
            if matched is not None:
                matched_gt.add(matched)

        # Also collect agent keys that don't match any gt (false positives)
        agent_unique_keys: list[str] = []
        remaining_gt = set(gt_keys)
        for agent_cause in agent_attribution:
            m = self._fuzzy_match_key(agent_cause, remaining_gt)
            if m is not None:
                remaining_gt.discard(m)
            else:
                ak = self._normalize_cause_key(agent_cause)
                agent_unique_keys.append(ak)

        # Build label vectors for F1
        # Universe = gt_keys UNION agent-only keys
        all_labels = list(gt_keys | set(agent_unique_keys))
        y_true = [1 if lbl in gt_keys else 0 for lbl in all_labels]
        y_pred = [1 if lbl in matched_gt or lbl in agent_unique_keys else 0 for lbl in all_labels]

        if sum(y_true) == 0 and sum(y_pred) == 0:
            return 1.0

        score = f1_score(y_true, y_pred, zero_division=0.0)
        return round(float(score), 4)

    def score_false_attribution(
        self,
        agent_attribution: list[dict],
        ground_truth: list[dict],
    ) -> float:
        """1 - (false_attributions / total_attributions).

        A false attribution is one where the agent claims a cause that
        doesn't exist in ground truth.

        Weight: 0.15, threshold: 0.90
        """
        if not agent_attribution:
            return 1.0

        gt_keys = {self._normalize_cause_key(c) for c in ground_truth}
        remaining_gt = set(gt_keys)
        false_count = 0

        for agent_cause in agent_attribution:
            matched = self._fuzzy_match_key(agent_cause, remaining_gt)
            if matched is not None:
                remaining_gt.discard(matched)
            else:
                false_count += 1

        total = len(agent_attribution)
        score = 1.0 - (false_count / total)
        return round(max(0.0, score), 4)

    def score_data_artifact_detection(
        self,
        agent_output: dict,
        is_data_artifact: bool,
    ) -> float:
        """Binary: did agent flag data quality issue when is_data_artifact=True?

        Check agent_output['data_quality_flags'] or attribution types containing
        'data_quality'.  Returns 1.0 if correctly identified, 0.0 if missed,
        1.0 if not a data artifact (N/A case).

        Weight: 0.10, threshold: 0.80
        """
        if not is_data_artifact:
            return 1.0

        # Check explicit data_quality_flags field
        dq_flags = agent_output.get("data_quality_flags", [])
        if dq_flags:
            return 1.0

        # Check if any attribution cause has type containing "data_quality" or "artifact"
        attributions = agent_output.get("attribution", [])
        if isinstance(attributions, dict):
            attributions = attributions.get("attribution", [])

        for cause in attributions:
            cause_type = cause.get("type", cause.get("cause_type", "")).lower()
            if "data_quality" in cause_type or "artifact" in cause_type or "data_issue" in cause_type:
                return 1.0

        # Check narrative text for data quality mentions as last resort
        narrative = json.dumps(agent_output).lower()
        data_quality_signals = [
            "data quality", "data artifact", "data issue", "logging error",
            "tracking issue", "instrumentation", "data anomal",
        ]
        for signal in data_quality_signals:
            if signal in narrative:
                return 1.0

        return 0.0

    def score_factual_grounding(self, grounding_result: dict) -> float:
        """Proportion of claims with valid source citation.

        Use grounding_score from grounding_checker output.

        Weight: 0.10, threshold: 0.95
        """
        if not grounding_result:
            return 0.0

        # Direct score if available
        grounding_score = grounding_result.get("grounding_score")
        if grounding_score is not None:
            return round(float(grounding_score), 4)

        # Compute from individual claims
        claims = grounding_result.get("claims", [])
        if not claims:
            return 0.0

        grounded = sum(1 for c in claims if c.get("grounded", False))
        score = grounded / len(claims)
        return round(score, 4)

    # ── aggregate ────────────────────────────────────────────────────────

    def compute_weighted_score(self, scores: dict) -> float:
        """Compute overall weighted score from individual dimension scores."""
        weights = {
            "attribution_accuracy": 0.30,
            "cause_identification": 0.25,
            "false_attribution": 0.15,
            "data_artifact_detection": 0.10,
            "narrative_quality": 0.10,
            "factual_grounding": 0.10,
        }
        total = sum(scores.get(k, 0) * w for k, w in weights.items())
        return round(total, 4)
