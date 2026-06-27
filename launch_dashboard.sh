#!/usr/bin/env bash
# Run this script from the repo root to serve the dashboard locally
# Usage: bash launch_dashboard.sh
set -e
NOTEBOOK="CS_MBA_AI_USAJobs_Jooble_Master_Combiner_v3_(2).ipynb"
echo "Starting Voilà dashboard on http://localhost:8866"
voila "$NOTEBOOK" \
  --no-browser=False \
  --port=8866 \
  --theme=light \
  --strip_sources=True
