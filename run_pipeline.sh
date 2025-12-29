#!/usr/bin/env bash
set -e

echo "Step 1: Extract"
python pipelines/01_extract.py

echo "Step 2: Transform + Load"
python pipelines/02_transform_load.py

echo "Step 3: Quality Checks"
python pipelines/03_quality_checks.py

echo "Step 4: Analytics Marts"
python pipelines/04_analytics_marts.py

echo "Step 5: Export Marts"
python pipelines/05_export_marts.py

echo "Pipeline finished successfully"
