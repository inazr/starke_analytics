from dagster import Definitions, EnvVar, load_assets_from_modules
from dagster_duckdb import DuckDBResource
import os
from .assets import assets, dbt
from .resources.dbt import dbt_resource
from .jobs import extract_and_transform_job, copy_for_superset_job, discover_network_job
from .sensors import copy_for_superset_after_dbt
from .schedules import extract_and_transform_every_4h

all_assets = load_assets_from_modules(modules=[assets, dbt])
all_jobs = [extract_and_transform_job, copy_for_superset_job, discover_network_job]

defs = Definitions(
    assets=all_assets,
    resources={
        "duckdb": DuckDBResource(
            database=EnvVar("DUCKDB_PATH"),
        ),
        "dbt": dbt_resource,
    },
    jobs=all_jobs,
    sensors=[copy_for_superset_after_dbt],
    schedules=[extract_and_transform_every_4h],
)
