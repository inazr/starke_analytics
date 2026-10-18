from dagster import DagsterRunStatus, DefaultSensorStatus, RunRequest, RunStatusSensorContext, run_status_sensor

from ..assets.dbt import dbt_analytics
from ..jobs import copy_for_superset_job


# Fires after every successful run that included dbt assets: the job extract_and_transform
# or a manual "Materialize" in the UI. A failed dbt build fails the run, so Superset keeps the old copy.
# Uses the asset keys of the dbt group, so new or renamed models need no change here.
@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    request_job=copy_for_superset_job,
    default_status=DefaultSensorStatus.RUNNING,
)
def copy_for_superset_after_dbt(context: RunStatusSensorContext):
    run_asset_keys = context.dagster_run.asset_selection or set()
    if run_asset_keys & set(dbt_analytics.keys):
        return RunRequest()
