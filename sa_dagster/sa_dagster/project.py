from dagster_dbt import DbtProject
from .assets.constants import DBT_DIRECTORY

dbt_project = DbtProject(
    project_dir=DBT_DIRECTORY,
)
# Under dg dev: run dbt parse on every code load, so model changes appear after "Reload".
# In production it does nothing; the manifest comes from the image build (TODO 24).
dbt_project.prepare_if_dev()
