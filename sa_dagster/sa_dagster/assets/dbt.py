from typing import Any, Mapping

from dagster import AssetExecutionContext
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets

from ..project import dbt_project


# One Dagster group per dbt layer, taken from the folder under models/ (staging, core, reporting),
# so new models need no group in their YAML. Seeds go into group "seeds".
# Set here and not as +meta in dbt_project.yml: dbt would replace a model's own meta.dagster (asset_key) with it.
class LayerGroupTranslator(DagsterDbtTranslator):
    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str:
        if dbt_resource_props["resource_type"] == "seed":
            return "seeds"
        # fqn of models/core/dim_receipts.sql is ["sa_dbt", "core", "dim_receipts"]
        return dbt_resource_props["fqn"][1]


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=LayerGroupTranslator(),
)
def dbt_analytics(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
