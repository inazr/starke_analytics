from dagster import Definitions, load_assets_from_modules
from dagster_duckdb import DuckDBResource
import os
from .assets import assets, dbt
from .resources.dbt import dbt_resource
from .assets.constants import DUCKDB_PATH

all_assets = load_assets_from_modules([assets, dbt])

defs = Definitions(
    assets=all_assets,
    resources={
        "duckdb": DuckDBResource(
            database=DUCKDB_PATH + '/starke_praxis.duckdb',
        ),
        "dbt": dbt_resource,
    },
)
