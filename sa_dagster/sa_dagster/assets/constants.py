import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

STARKE_PRAXIS_PORT = int(os.getenv('STARKE_PRAXIS_PORT'))
STARKE_PRAXIS_USER = os.getenv('STARKE_PRAXIS_USER')
STARKE_PRAXIS_PASSWORD = os.getenv('STARKE_PRAXIS_PASSWORD')

CONFIG_FILE_PATH = os.getenv('CONFIG_FILE_PATH')

DUCKDB_PATH = os.getenv('DUCKDB_PATH')
SUPERSET_DUCKDB_PATH = os.getenv('SUPERSET_DUCKDB_PATH')

# Fixed location inside the repo: assets/ -> sa_dagster/ (package) -> sa_dagster/ -> repo root
PROJECT_ROOT = Path(__file__).resolve().parents[3]
DBT_DIRECTORY = PROJECT_ROOT / 'sa_dbt'
