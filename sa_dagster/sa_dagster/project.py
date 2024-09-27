from dagster_dbt import DbtProject
from .assets.constants import DBT_DIRECTORY

dbt_project = DbtProject(
    project_dir=DBT_DIRECTORY,
)
