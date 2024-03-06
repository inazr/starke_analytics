from dagster import Definitions, load_assets_from_modules
from dagster_duckdb import DuckDBResource
import os
from . import assets
from dotenv import load_dotenv
load_dotenv()

DUCKDB_PATH = os.getenv('DUCKDB_PATH')

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        "duckdb": DuckDBResource(
            database=DUCKDB_PATH + '/starke_praxis.duckdb',
        )
    },
)
