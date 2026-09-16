#!/bin/bash
# Extract every cell log into the results file and render the figures.
set -e
cd "$(dirname "$0")"
python3 fu_extract.py logs results/fu_results.json
python3 fu_figures.py results/fu_results.json figures
