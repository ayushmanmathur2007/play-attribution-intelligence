"""Stage 5: Verify attribution claims against source data."""

import json
import logging
import re
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


class GroundingChecker:
    """Verify each attribution claim against the actual source data,
    combining deterministic checks with an LLM-based nuanced review.
    """

    def __init__(self, llm_client):
        self.llm = llm_client
        self.prompt_template = (
            Path(__file__).parent / "prompts" / "grounding_check.txt"
        ).read_text()

    async def check(self, attribution: dict, fetched_data: dict) -> dict:
        """Verify each attribution claim against source data.

        Returns dict with keys:
            verified_claims (list), grounding_score (float),
            critical_issues (list), recommendations (list).
        Also includes the original attribution fields, merged with
        grounding results.
        """
        # Phase 1: Deterministic checks
        deterministic_results = self._deterministic_checks(attribution, fetched_data)

        # Phase 2: LLM-based nuanced verification
        llm_results = await self._llm_check(attribution, fetched_data, deterministic_results)

        # Merge results
        merged = self._merge_results(attribution, deterministic_results, llm_results)
        return merged

    # ------------------------------------------------------------------
    # Phase 1: Deterministic checks
    # ------------------------------------------------------------------

    def _deterministic_checks(
        self, attribution: dict, fetched_data: dict
    ) -> list[dict]:
        """Run rule-based verification on each claim."""
        initiatives_df = fetched_data.get("initiatives")
        confounders_df = fetched_data.get("confounders")

        results = []
        for claim in attribution.get("attribution", []):
            issues = []
            status = "VERIFIED"

            # Check 1: Initiative existence
            init_id = claim.get("initiative_id")
            if init_id and claim.get("type") == "initiative":
                if not self._initiative_exists(init_id, initiatives_df):
                    issues.append(
                        f"Initiative '{init_id}' not found in initiative_calendar."
                    )
                    status = "UNGROUNDED"
                else:
                    # Check date alignment and dimension match
                    init_row = self._get_initiative(init_id, initiatives_df)
                    if init_row is not None:
                        date_issues = self._check_date_alignment(
                            init_row, attribution
                        )
                        issues.extend(date_issues)
                        dim_issues = self._check_dimension_match(
                            init_row, attribution
                        )
                        issues.extend(dim_issues)
                        dir_issues = self._check_direction_consistency(
                            init_row, claim
                        )
                        issues.extend(dir_issues)

            # Check 2: Confounder claims
            if claim.get("type") == "confounder":
                confounder_name = claim.get("cause", "")
                if not self._confounder_plausible(confounder_name, confounders_df):
                    issues.append(
                        f"Confounder '{confounder_name}' not found in confounder_log."
                    )
                    status = "UNGROUNDED"

            # Check 3: Contribution percentage sanity
            pct = claim.get("contribution_pct", 0)
            if pct < 0 or pct > 1.0:
                issues.append(
                    f"Contribution percentage {pct} is outside valid range [0, 1]."
                )
                if status == "VERIFIED":
                    status = "PARTIALLY_VERIFIED"

            if issues and status == "VERIFIED":
                status = "PARTIALLY_VERIFIED"

            results.append({
                "cause": claim.get("cause", "Unknown"),
                "status": status,
                "issues": issues,
                "contribution_pct": claim.get("contribution_pct", 0),
                "deterministic_pass": len(issues) == 0,
            })

        return results

    def _initiative_exists(
        self, init_id: str, initiatives_df: pd.DataFrame | None
    ) -> bool:
        """Check if an initiative ID exists in the calendar."""
        if initiatives_df is None or initiatives_df.empty:
            return False
        if "initiative_id" not in initiatives_df.columns:
            return False
        return init_id in initiatives_df["initiative_id"].values

    def _get_initiative(
        self, init_id: str, initiatives_df: pd.DataFrame | None
    ) -> pd.Series | None:
        """Get a single initiative row by ID."""
        if initiatives_df is None or initiatives_df.empty:
            return None
        if "initiative_id" not in initiatives_df.columns:
            return None
        matches = initiatives_df[initiatives_df["initiative_id"] == init_id]
        if matches.empty:
            return None
        return matches.iloc[0]

    def _check_date_alignment(
        self, init_row: pd.Series, attribution: dict
    ) -> list[str]:
        """Verify the initiative was active during the attribution period."""
        issues = []
        # The attribution dict may not have period directly; check for common keys
        # We don't have the parsed_query here directly, so we check what's available
        period = attribution.get("period", {})
        if not period:
            return issues

        try:
            query_start = str(period.get("start_date", ""))
            query_end = str(period.get("end_date", ""))
            init_start = str(init_row.get("start_date", ""))
            init_end = str(init_row.get("end_date", ""))

            if init_start and query_end and init_start > query_end:
                issues.append(
                    f"Initiative starts ({init_start}) after query period ends ({query_end})."
                )
            if init_end and query_start and init_end < query_start:
                issues.append(
                    f"Initiative ended ({init_end}) before query period starts ({query_start})."
                )
        except Exception as e:
            logger.debug("Date alignment check error: %s", e)

        return issues

    def _check_dimension_match(
        self, init_row: pd.Series, attribution: dict
    ) -> list[str]:
        """Verify initiative targets the same market/category/segment."""
        issues = []

        for dim in ["market_id", "category_id", "segment_id"]:
            attr_key = dim.replace("_id", "")  # e.g., "market"
            attr_val = attribution.get(attr_key, "ALL")
            if attr_val == "ALL":
                continue

            init_val = init_row.get(dim)
            if pd.isna(init_val) or str(init_val).strip().upper() == "ALL":
                continue  # Initiative targets all — compatible

            init_values = [v.strip() for v in str(init_val).split(",")]
            if attr_val not in init_values:
                issues.append(
                    f"Initiative targets {dim}={init_val}, "
                    f"but attribution is for {attr_key}={attr_val}."
                )

        return issues

    def _check_direction_consistency(
        self, init_row: pd.Series, claim: dict
    ) -> list[str]:
        """Check that an initiative designed to boost a metric isn't attributed
        to a decline (and vice versa).
        """
        issues = []
        expected_impact = str(init_row.get("expected_impact", "")).lower()
        # Infer direction from claim evidence or parent attribution direction
        claim_evidence = str(claim.get("evidence", "")).lower()

        if "increase" in expected_impact or "boost" in expected_impact:
            if "decrease" in claim_evidence or "decline" in claim_evidence or "drop" in claim_evidence:
                issues.append(
                    "Direction mismatch: initiative expected to increase metric, "
                    "but claim describes a decrease."
                )
        elif "decrease" in expected_impact or "reduce" in expected_impact:
            if "increase" in claim_evidence or "growth" in claim_evidence or "spike" in claim_evidence:
                issues.append(
                    "Direction mismatch: initiative expected to decrease metric, "
                    "but claim describes an increase."
                )

        return issues

    def _confounder_plausible(
        self, confounder_name: str, confounders_df: pd.DataFrame | None
    ) -> bool:
        """Check if a named confounder exists in the log."""
        if confounders_df is None or confounders_df.empty:
            return False

        # Check by name (fuzzy — lowercase comparison)
        name_lower = confounder_name.lower()
        for col in ["name", "description", "confounder_id"]:
            if col in confounders_df.columns:
                matches = confounders_df[col].astype(str).str.lower()
                if matches.str.contains(name_lower, na=False).any():
                    return True

        return False

    # ------------------------------------------------------------------
    # Phase 2: LLM-based nuanced checks
    # ------------------------------------------------------------------

    async def _llm_check(
        self,
        attribution: dict,
        fetched_data: dict,
        deterministic_results: list[dict],
    ) -> dict:
        """Use the LLM for nuanced grounding checks that rules can't cover."""
        # Format source data for the prompt
        source_data_str = self._format_source_data(fetched_data)
        initiative_str = self._df_to_string(fetched_data.get("initiatives"))
        confounder_str = self._df_to_string(fetched_data.get("confounders"))

        # Include deterministic findings in the prompt
        attribution_with_det = {
            **attribution,
            "_deterministic_findings": deterministic_results,
        }

        system_prompt = self.prompt_template
        for placeholder, value in {
            "{attribution_json}": json.dumps(attribution_with_det, indent=2, default=str),
            "{source_data}": source_data_str,
            "{initiative_calendar}": initiative_str,
            "{confounder_log}": confounder_str,
        }.items():
            system_prompt = system_prompt.replace(placeholder, str(value))

        user_prompt = (
            "Verify each attribution claim against the source data. "
            "Pay special attention to any deterministic issues already flagged. "
            "Return your assessment as JSON."
        )

        response = await self.llm.complete(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            temperature=0.1,
            max_tokens=4096,
            response_format="json",
        )

        return self._parse_llm_response(response)

    def _format_source_data(self, fetched_data: dict) -> str:
        """Format the primary and adjacent data for the grounding prompt."""
        sections = []
        for key in ["primary_data", "adjacent_metrics", "cross_market"]:
            df = fetched_data.get(key)
            if df is not None and isinstance(df, pd.DataFrame) and not df.empty:
                # Limit to 50 rows to avoid prompt bloat
                display = df.head(50)
                sections.append(f"### {key}\n{display.to_markdown(index=False)}")

        return "\n\n".join(sections) if sections else "No source data available."

    def _df_to_string(self, df: pd.DataFrame | None) -> str:
        """Convert a DataFrame to a markdown table string."""
        if df is None or not isinstance(df, pd.DataFrame) or df.empty:
            return "No data available."
        return df.head(30).to_markdown(index=False)

    def _parse_llm_response(self, response: str) -> dict:
        """Parse the grounding check LLM response."""
        text = response.strip()
        if text.startswith("```"):
            text = re.sub(r"^```(?:json)?\s*", "", text)
            text = re.sub(r"\s*```$", "", text)

        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            json_match = re.search(r"\{[\s\S]*\}", text)
            if json_match:
                try:
                    parsed = json.loads(json_match.group(0))
                except json.JSONDecodeError:
                    parsed = {}
            else:
                parsed = {}

        parsed.setdefault("verified_claims", [])
        parsed.setdefault("grounding_score", 0.5)
        parsed.setdefault("critical_issues", [])
        parsed.setdefault("recommendations", [])

        return parsed

    # ------------------------------------------------------------------
    # Merge deterministic + LLM results
    # ------------------------------------------------------------------

    def _merge_results(
        self,
        attribution: dict,
        deterministic_results: list[dict],
        llm_results: dict,
    ) -> dict:
        """Merge deterministic check results with LLM verification into
        a unified grounding output.
        """
        llm_claims = {
            c.get("cause", ""): c
            for c in llm_results.get("verified_claims", [])
        }

        verified_claims = []
        for det in deterministic_results:
            cause = det["cause"]
            llm_claim = llm_claims.get(cause, {})

            # Combine issues from both sources
            all_issues = list(det.get("issues", []))
            all_issues.extend(llm_claim.get("issues", []))

            # Use the more conservative status
            det_status = det.get("status", "VERIFIED")
            llm_status = llm_claim.get("status", det_status)
            final_status = self._conservative_status(det_status, llm_status)

            # Use LLM's corrected contribution if available
            corrected_pct = llm_claim.get(
                "corrected_contribution_pct",
                det.get("contribution_pct", 0),
            )

            verified_claims.append({
                "cause": cause,
                "status": final_status,
                "issues": all_issues,
                "corrected_contribution_pct": corrected_pct,
            })

        # Add any LLM-only claims (claims the LLM found that deterministic didn't)
        det_causes = {d["cause"] for d in deterministic_results}
        for cause, claim in llm_claims.items():
            if cause not in det_causes:
                verified_claims.append({
                    "cause": cause,
                    "status": claim.get("status", "PARTIALLY_VERIFIED"),
                    "issues": claim.get("issues", []),
                    "corrected_contribution_pct": claim.get(
                        "corrected_contribution_pct", 0
                    ),
                })

        # Compute grounding score
        grounding_score = self._compute_grounding_score(
            verified_claims, llm_results.get("grounding_score", 0.5)
        )

        # Combine critical issues
        critical_issues = list(llm_results.get("critical_issues", []))
        for det in deterministic_results:
            if det.get("status") == "UNGROUNDED":
                critical_issues.append(
                    f"Claim '{det['cause']}' failed deterministic checks: "
                    + "; ".join(det.get("issues", []))
                )

        return {
            # Carry over the original attribution fields
            "movement_confirmed": attribution.get("movement_confirmed", True),
            "magnitude": attribution.get("magnitude", "unknown"),
            "attribution": attribution.get("attribution", []),
            "ruled_out": attribution.get("ruled_out", []),
            "data_quality_flags": attribution.get("data_quality_flags", []),
            "overall_confidence": attribution.get("overall_confidence", "medium"),
            # Grounding-specific fields
            "verified_claims": verified_claims,
            "grounding_score": grounding_score,
            "critical_issues": critical_issues,
            "recommendations": llm_results.get("recommendations", []),
        }

    def _conservative_status(self, a: str, b: str) -> str:
        """Return the more conservative of two statuses."""
        priority = {"UNGROUNDED": 0, "PARTIALLY_VERIFIED": 1, "VERIFIED": 2}
        a_p = priority.get(a, 1)
        b_p = priority.get(b, 1)
        if a_p <= b_p:
            return a
        return b

    def _compute_grounding_score(
        self, verified_claims: list[dict], llm_score: float
    ) -> float:
        """Compute a weighted grounding score from claim statuses and the
        LLM's own assessment.
        """
        if not verified_claims:
            return llm_score

        status_weights = {
            "VERIFIED": 1.0,
            "PARTIALLY_VERIFIED": 0.5,
            "UNGROUNDED": 0.0,
        }

        claim_scores = []
        for c in verified_claims:
            weight = abs(c.get("corrected_contribution_pct", 0))
            score = status_weights.get(c["status"], 0.5)
            claim_scores.append((score, max(weight, 0.01)))

        total_weight = sum(w for _, w in claim_scores)
        if total_weight == 0:
            deterministic_score = 0.5
        else:
            deterministic_score = sum(s * w for s, w in claim_scores) / total_weight

        # Blend: 60% deterministic, 40% LLM assessment
        return round(0.6 * deterministic_score + 0.4 * llm_score, 3)
