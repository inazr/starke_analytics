from dagster import ScheduleDefinition
from ..jobs import run_dbt_stg_starke_job

trip_update_schedule = ScheduleDefinition(
    job=run_dbt_stg_starke_job,
    cron_schedule="0 * * * *", # hourly
)
