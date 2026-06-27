#!/usr/bin/env bash
set -e
NOTEBOOK="CS_MBA_AI_USAJobs_Jooble_Master_Combiner_v3_Dashboard.ipynb"
echo "Starting Voila dashboard on http://localhost:8866"
voila "$NOTEBOOK" --no-browser=False --port=8866 --theme=light --strip_sources=True
