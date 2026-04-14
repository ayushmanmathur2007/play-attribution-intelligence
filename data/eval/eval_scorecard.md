# Eval Scorecard

**Generated:** 2026-04-11T23:13:42.076903
**Total cases:** 32
**Overall weighted score:** 0.6290

**Verdict:** FAIL

## Per-Dimension Scores

| Dimension | Weight | Threshold | Mean | Median | Min | Max | Pass Rate |
|-----------|--------|-----------|------|--------|-----|-----|-----------|
| + Attribution Accuracy | 0.30 | 0.70 | 0.9958 | 0.9956 | 0.9943 | 0.9985 | 100% |
| - Cause Identification | 0.25 | 0.80 | 0.2708 | 0.3667 | 0.0000 | 0.8000 | 3% |
| - False Attribution | 0.15 | 0.90 | 0.4219 | 0.3333 | 0.0000 | 1.0000 | 19% |
| + Data Artifact Detection | 0.10 | 0.80 | 1.0000 | 1.0000 | 1.0000 | 1.0000 | 100% |
| - Narrative Quality | 0.10 | 0.70 | 0.4575 | 0.4000 | 0.3200 | 0.8800 | 3% |
| - Factual Grounding | 0.10 | 0.95 | 0.5356 | 0.6230 | 0.0000 | 0.8600 | 0% |

## Per-Difficulty Breakdown

### MEDIUM (n=20)
- Mean weighted score: 0.5796
  - Attribution Accuracy: 0.9956
  - Cause Identification: 0.1700
  - False Attribution: 0.2500
  - Data Artifact Detection: 1.0000
  - Narrative Quality: 0.4500
  - Factual Grounding: 0.5593

### HARD (n=12)
- Mean weighted score: 0.7114
  - Attribution Accuracy: 0.9961
  - Cause Identification: 0.4389
  - False Attribution: 0.7083
  - Data Artifact Detection: 1.0000
  - Narrative Quality: 0.4700
  - Factual Grounding: 0.4960

## Top 5 Worst Cases

| # | Movement ID | Difficulty | Metric | Market | Score | Failing Dimensions |
|---|-------------|------------|--------|--------|-------|--------------------|
| 1 | MOV_CF286F0309 | MEDIUM | dau | PH | 0.4795 | cause_identification, false_attribution, narrative_quality, factual_grounding |
| 2 | MOV_A449E0339B | MEDIUM | offer_redemption_count | VN | 0.4835 | cause_identification, false_attribution, narrative_quality, factual_grounding |
| 3 | MOV_E8FEB942C7 | MEDIUM | offer_driven_revenue | IN | 0.4875 | cause_identification, false_attribution, narrative_quality, factual_grounding |
| 4 | MOV_98DADE9915 | MEDIUM | revenue_per_user | US | 0.4885 | cause_identification, false_attribution, narrative_quality, factual_grounding |
| 5 | MOV_B298D9C374 | MEDIUM | offer_ctr | MX | 0.4928 | cause_identification, false_attribution, narrative_quality, factual_grounding |

## Recommendations

- Cause Identification is 0.53 below threshold (0.27 vs 0.80). Pass rate is only 3%.
- False Attribution is 0.48 below threshold (0.42 vs 0.90). Pass rate is only 19%.
- Narrative Quality is 0.24 below threshold (0.46 vs 0.70). Pass rate is only 3%.
- Factual Grounding is 0.41 below threshold (0.54 vs 0.95). Pass rate is only 0%.
- Agent is hallucinating causes. Add explicit instruction to only attribute causes supported by data evidence.
- Factual grounding is below threshold. Tighten the grounding checker to require explicit source references for every claim.
- Narrative quality is low. Review the narrative generator prompt for clarity and actionability improvements.
