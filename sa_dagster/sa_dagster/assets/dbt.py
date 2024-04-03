from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import dbt_assets, DbtCliResource, DagsterDbtTranslator

import os

from .constants import DBT_DIRECTORY

dbt_manifest_path = os.path.join(DBT_DIRECTORY, "target", "manifest.json")


class CustomizedDagsterDbtTranslator(DagsterDbtTranslator):

    @classmethod
    def get_asset_key(cls, dbt_resource_props):
        resource_type = dbt_resource_props["resource_type"]
        name = dbt_resource_props["name"]

        if resource_type == "source":
            return AssetKey(f"{name}")
        else:
            return super().get_asset_key(dbt_resource_props)


@dbt_assets(
    manifest=dbt_manifest_path,
    dagster_dbt_translator=CustomizedDagsterDbtTranslator()
)
def dbt_analytics(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["run"], context=context).stream()
