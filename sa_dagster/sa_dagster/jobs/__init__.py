from dagster import define_asset_job, AssetSelection
from dagster_dbt import build_dbt_asset_selection
from ..assets.dbt import dbt_analytics

run_dbt_stg_starke = build_dbt_asset_selection([dbt_analytics], "tag:stg_starke+")

run_dbt_stg_starke_job = define_asset_job(
    name="dbt_run_stg_starke",
    selection=run_dbt_stg_starke,
)

discover_network = AssetSelection.groups("discover_network")

discover_network_job = define_asset_job(
    name="discover_network",
    selection=discover_network
)

sync_data_and_build_dbt = AssetSelection.groups("extraction_entry_point", "extract_load_starke")

sync_data_and_build_dbt_job = define_asset_job(
    name="sync_data_and_build_models",
    selection=sync_data_and_build_dbt
)
