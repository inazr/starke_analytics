from dagster import define_asset_job, AssetSelection

from ..assets.dbt import dbt_analytics

# Extract from Starke, then all dbt assets (every layer group, see assets/dbt.py).
# The Superset copy is its own job, started by hand.
# Group names: assets.py
extract_and_transform = AssetSelection.groups(
    "extraction_entry_point", "extract_from_starke"
) | AssetSelection.assets(dbt_analytics)

extract_and_transform_job = define_asset_job(
    name="extract_and_transform",
    selection=extract_and_transform,
)

copy_for_superset = AssetSelection.groups("superset")

copy_for_superset_job = define_asset_job(
    name="copy_for_superset",
    selection=copy_for_superset,
)

discover_network = AssetSelection.groups("discover_network")

discover_network_job = define_asset_job(
    name="discover_network",
    selection=discover_network
)
