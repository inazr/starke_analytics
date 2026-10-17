#!/bin/bash
# Start Dagster. Run from anywhere: ./dagster_dev_startup.sh
cd "$(dirname "$0")"
export DAGSTER_HOME="$PWD/sa_dagster"
uv run dagster dev
