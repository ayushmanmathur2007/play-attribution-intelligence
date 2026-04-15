#!/usr/bin/env bash
# One-command bootstrap: generate events, run all pipeline stages,
# and report what got built.
#
# Usage (from project root):
#   bash scripts/bootstrap.sh
#
# Env:
#   SKIP_GENERATE=1   reuse existing data/raw/* (faster iteration)
#   ANTHROPIC_API_KEY pass-through if you want LLM narrative polish
set -euo pipefail

cd "$(dirname "$0")/.."

echo "==> python: $(python --version)"
echo "==> cwd:    $(pwd)"

if [[ -z "${VIRTUAL_ENV:-}" ]]; then
  echo "WARNING: no virtualenv active. Install requirements into an isolated env."
fi

echo
echo "==> step 1/3: install requirements"
pip install -q -r requirements.txt

if [[ "${SKIP_GENERATE:-0}" != "1" ]]; then
  echo
  echo "==> step 2/3: generate synthetic clickstream"
  python -m src.generator.run
else
  echo
  echo "==> step 2/3: SKIP_GENERATE=1, reusing existing data/raw/*"
fi

echo
echo "==> step 3/3: build the 5-layer pyramid"
python -m src.pipeline.build

echo
echo "==> done. launch the demo with:"
echo "    streamlit run src/ui/app.py"
