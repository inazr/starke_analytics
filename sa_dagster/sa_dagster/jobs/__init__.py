from dagster import define_asset_job, AssetSelection
from dagster_dbt import build_dbt_asset_selection
from ..assets.dbt import dbt_analytics

run_dbt_stg_starke = build_dbt_asset_selection([dbt_analytics], "tag:stg_starke+")
run_dbt_stg_starke_job = define_asset_job(
    name="dbt_run_stg_starke",
    selection=run_dbt_stg_starke,
)

extract_load_starke_praxis = AssetSelection.groups("extract_load_starke")

extract_load_starke_praxis_job = define_asset_job(
    name="extract_load_starke_praxis",
    selection=extract_load_starke_praxis
)
