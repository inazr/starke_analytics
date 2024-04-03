import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

DUCKDB_PATH = os.getenv('DUCKDB_PATH')

STARKE_PRAXIS_PORT = int(os.getenv('STARKE_PRAXIS_PORT'))
STARKE_PRAXIS_USER = os.getenv('STARKE_PRAXIS_USER')
STARKE_PRAXIS_PASSWORD = os.getenv('STARKE_PRAXIS_PASSWORD')

CONFIG_FILE_PATH = os.getenv('CONFIG_FILE_PATH')

DBT_DIRECTORY = Path(__file__).joinpath("../..", "..", "..", "sa_dbt").resolve()
