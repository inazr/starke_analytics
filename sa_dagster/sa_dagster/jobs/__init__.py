from dagster import define_asset_job, AssetSelection

# Extract from Starke, dbt staging + core, then copy the DuckDB file for Superset.
# Group names: assets.py, staging.yml, core.yml
extract_and_transform = AssetSelection.groups(
    "extraction_entry_point", "extract_from_starke", "staging", "core", "superset"
)

extract_and_transform_job = define_asset_job(
    name="extract_and_transform",
    selection=extract_and_transform,
)

discover_network = AssetSelection.groups("discover_network")

discover_network_job = define_asset_job(
    name="discover_network",
    selection=discover_network
)
