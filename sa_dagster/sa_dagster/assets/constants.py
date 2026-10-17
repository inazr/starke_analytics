import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

STARKE_PRAXIS_PORT = int(os.getenv('STARKE_PRAXIS_PORT'))
STARKE_PRAXIS_USER = os.getenv('STARKE_PRAXIS_USER')
STARKE_PRAXIS_PASSWORD = os.getenv('STARKE_PRAXIS_PASSWORD')

# Fixed locations inside the repo: assets/ -> sa_dagster/ (package) -> sa_dagster/ -> repo root
PROJECT_ROOT = Path(__file__).resolve().parents[3]
CONFIG_FILE_PATH = PROJECT_ROOT / 'config.ini'
DUCKDB_FILE = PROJECT_ROOT / 'duckdb' / 'starke_praxis.duckdb'
DBT_DIRECTORY = PROJECT_ROOT / 'sa_dbt'
