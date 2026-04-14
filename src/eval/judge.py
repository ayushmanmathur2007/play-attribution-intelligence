"""LLM-as-judge for narrative quality evaluation."""

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

_DIMENSIONS = ("clarity", "actionability", "completeness", "tone", "structure")
_MAX_SCORE = 5


class LLMJudge:
    """Use an LLM to score narrative quality on 5 dimensions (1-5 each)."""

    def __init__(self, llm_client):
        self.llm = llm_client
        prompt_path = Path(__file__).parent.parent / "agent" / "prompts" / "eval_judge.txt"
        self.prompt_template = prompt_path.read_text()

    async def score_narrative(
        self,
        agent_narrative: str,
        ground_truth_narrative: str,
        agent_attribution: list,
        ground_truth_attribution: list,
    ) -> dict:
        """Score narrative quality on 5 dimensions using LLM-as-judge.

        Returns
        -------
        dict
            {
                "dimensions": {
                    "clarity":       {"score": int, "reason": str},
                    "actionability": {"score": int, "reason": str},
                    ...
                },
                "average_score": float   # normalised to 0-1
            }
        """
        filled_prompt = self.prompt_template
        for placeholder, value in {
            "{agent_narrative}": agent_narrative or "(no narrative produced)",
            "{ground_truth_narrative}": ground_truth_narrative or "(none provided)",
            "{ground_truth_attribution}": json.dumps(ground_truth_attribution, indent=2),
            "{agent_attribution}": json.dumps(agent_attribution, indent=2),
        }.items():
            filled_prompt = filled_prompt.replace(placeholder, str(value))

        system_prompt = (
            "You are a strict but fair evaluator of AI-generated metric attribution narratives. "
            "Score each dimension on a 1-5 integer scale with a one-sentence justification."
        )

        try:
            raw_response = await self.llm.complete(
                system_prompt=system_prompt,
                user_prompt=filled_prompt,
                temperature=0.0,
                max_tokens=1024,
                response_format="json",
            )
            parsed = self._parse_response(raw_response)
        except Exception:
            logger.exception("LLM judge call failed; returning default mid-range scores")
            parsed = self._default_scores()

        # Compute normalised average (0-1 scale)
        dim_scores = [
            parsed.get(dim, {}).get("score", 3) for dim in _DIMENSIONS
        ]
        average = sum(dim_scores) / (len(_DIMENSIONS) * _MAX_SCORE)

        return {
            "dimensions": parsed,
            "average_score": round(average, 4),
        }

    # ── internal helpers ─────────────────────────────────────────────────

    @staticmethod
    def _parse_response(raw: str) -> dict:
        """Parse LLM JSON response, clamping scores to 1-5."""
        # Strip markdown fences if present
        text = raw.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1]
        if text.endswith("```"):
            text = text.rsplit("```", 1)[0]
        text = text.strip()

        data = json.loads(text)

        result: dict = {}
        for dim in _DIMENSIONS:
            entry = data.get(dim, {})
            score = int(entry.get("score", 3))
            score = max(1, min(_MAX_SCORE, score))
            reason = str(entry.get("reason", "No justification provided"))
            result[dim] = {"score": score, "reason": reason}
        return result

    @staticmethod
    def _default_scores() -> dict:
        """Fallback mid-range scores used when the LLM call fails."""
        return {
            dim: {"score": 3, "reason": "Default score (LLM judge call failed)"}
            for dim in _DIMENSIONS
        }
