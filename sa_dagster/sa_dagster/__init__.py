from dagster import Definitions, EnvVar, load_assets_from_modules
from dagster_duckdb import DuckDBResource
import os
from .assets import assets, dbt
from .resources.dbt import dbt_resource
from .jobs import extract_and_transform_job, discover_network_job

all_assets = load_assets_from_modules(modules=[assets, dbt])
all_jobs = [extract_and_transform_job, discover_network_job]

defs = Definitions(
    assets=all_assets,
    resources={
        "duckdb": DuckDBResource(
            database=EnvVar("DUCKDB_PATH"),
        ),
        "dbt": dbt_resource,
    },
    jobs=all_jobs,
)
