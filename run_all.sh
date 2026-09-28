#!/usr/bin/env bash
# Relance tout le pipeline depuis la racine du projet : ./run_all.sh
set -e
cd "$(dirname "$0")"
source .venv/bin/activate
python tests/test_regles.py
python src/00_exploration.py
python src/01_ingestion.py
python src/02_rdd_indicateurs.py
python src/03_dataframe_parquet.py
python src/04_plan_benchmark.py
python src/05_graphe.py
