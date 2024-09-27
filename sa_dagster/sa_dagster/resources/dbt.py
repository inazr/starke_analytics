from dagster_dbt import DbtCliResource
from ..project import dbt_project

dbt_resource = DbtCliResource(
    project_dir=dbt_project,
)
