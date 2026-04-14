"""Stage 4: Core LLM reasoning step — produce structured attribution."""

import json
import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)


class AttributionReasoner:
    """Use the LLM to analyze enriched context and produce a structured
    attribution of the metric movement.
    """

    def __init__(self, llm_client):
        self.llm = llm_client
        self.prompt_template = (
            Path(__file__).parent / "prompts" / "attribution_reasoner.txt"
        ).read_text()

    async def reason(self, enriched_context: dict) -> dict:
        """Core LLM reasoning step — produce structured attribution.

        Takes the enriched context and generates attribution hypotheses.
        Returns dict with keys:
            movement_confirmed, magnitude, attribution (list),
            ruled_out (list), data_quality_flags (list),
            overall_confidence.
        """
        system_prompt = self._build_prompt(enriched_context)
        user_prompt = (
            f"Analyze the movement of {enriched_context.get('metric', 'the metric')} "
            f"in {enriched_context.get('market', 'ALL')} market "
            f"during {enriched_context.get('period', {}).get('description', 'the specified period')}. "
            f"Produce a structured attribution."
        )

        response = await self.llm.complete(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            temperature=0.2,
            max_tokens=4096,
            response_format="json",
        )

        attribution = self._parse_response(response)
        attribution = self._validate_attribution(attribution)
        return attribution

    def _build_prompt(self, ctx: dict) -> str:
        """Fill the prompt template with enriched context values."""
        replacements = {
            "{movement_summary}": ctx.get("movement_summary", "No movement summary available."),
            "{fetched_data_tables}": ctx.get("fetched_data_tables", "No data tables available."),
            "{initiative_list}": ctx.get("initiative_details", "No active initiatives."),
            "{seasonal_context}": ctx.get("seasonal_context", "No seasonal data available."),
            "{confounder_list}": ctx.get("confounder_details", "No known confounders."),
            "{metric_definition}": ctx.get("metric_definition", "No metric definition available."),
            "{adjacent_metrics}": ctx.get("adjacent_metrics_summary", "No adjacent metrics."),
            "{change_points}": ctx.get("change_point_summary", "No change points detected."),
        }
        result = self.prompt_template
        for placeholder, value in replacements.items():
            result = result.replace(placeholder, str(value))
        return result

    def _parse_response(self, response: str) -> dict:
        """Parse the LLM JSON response, handling common formatting issues."""
        text = response.strip()

        # Strip markdown code fences if present
        if text.startswith("```"):
            text = re.sub(r"^```(?:json)?\s*", "", text)
            text = re.sub(r"\s*```$", "", text)

        try:
            return json.loads(text)
        except json.JSONDecodeError as e:
            logger.warning("Failed to parse attribution JSON: %s", e)
            # Try to extract JSON from mixed text
            json_match = re.search(r"\{[\s\S]*\}", text)
            if json_match:
                try:
                    return json.loads(json_match.group(0))
                except json.JSONDecodeError:
                    pass

            # Return a structured error response
            return {
                "movement_confirmed": False,
                "magnitude": "Unable to parse LLM response",
                "attribution": [],
                "ruled_out": [],
                "data_quality_flags": [f"LLM response parsing failed: {str(e)[:100]}"],
                "overall_confidence": "low",
                "_raw_response": text[:500],
            }

    def _validate_attribution(self, attribution: dict) -> dict:
        """Validate and normalize the attribution output."""
        # Ensure required keys exist
        attribution.setdefault("movement_confirmed", True)
        attribution.setdefault("magnitude", "unknown")
        attribution.setdefault("attribution", [])
        attribution.setdefault("ruled_out", [])
        attribution.setdefault("data_quality_flags", [])
        attribution.setdefault("overall_confidence", "medium")

        # Validate attribution list entries
        valid_types = {"initiative", "seasonal", "confounder", "organic", "data_quality"}
        valid_confidences = {"high", "medium", "low"}

        cleaned_attributions = []
        for item in attribution["attribution"]:
            if not isinstance(item, dict):
                continue

            # Ensure required fields
            item.setdefault("cause", "Unknown cause")
            item.setdefault("type", "organic")
            item.setdefault("contribution_pct", 0.0)
            item.setdefault("confidence", "medium")
            item.setdefault("evidence", "No evidence cited.")
            item.setdefault("initiative_id", None)

            # Normalize type
            if item["type"] not in valid_types:
                item["type"] = "organic"

            # Normalize confidence
            if item["confidence"] not in valid_confidences:
                item["confidence"] = "medium"

            # Ensure contribution_pct is numeric
            try:
                item["contribution_pct"] = float(item["contribution_pct"])
            except (ValueError, TypeError):
                item["contribution_pct"] = 0.0

            cleaned_attributions.append(item)

        attribution["attribution"] = cleaned_attributions

        # Check that contributions sum to ~100%
        if cleaned_attributions:
            total = sum(a["contribution_pct"] for a in cleaned_attributions)
            if total > 0 and abs(total - 1.0) > 0.15:
                # Contributions are likely expressed as percentages (0-100) not fractions (0-1)
                if total > 50:
                    # Normalize from percentage to fraction
                    for a in cleaned_attributions:
                        a["contribution_pct"] = a["contribution_pct"] / 100.0
                    total = sum(a["contribution_pct"] for a in cleaned_attributions)

                if abs(total - 1.0) > 0.15:
                    attribution["data_quality_flags"].append(
                        f"Attribution contributions sum to {total:.1%}, not ~100%. "
                        f"Results may be unreliable."
                    )

        # Normalize overall_confidence
        if attribution["overall_confidence"] not in valid_confidences:
            attribution["overall_confidence"] = "medium"

        return attribution
