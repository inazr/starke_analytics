from dagster import DefaultScheduleStatus, ScheduleDefinition

from ..jobs import extract_and_transform_job


# At 00:00, 04:00, 08:00, 12:00, 16:00 and 20:00 German time.
extract_and_transform_every_4h = ScheduleDefinition(
    job=extract_and_transform_job,
    cron_schedule="0 */4 * * *",
    execution_timezone="Europe/Berlin",
    default_status=DefaultScheduleStatus.RUNNING,
)
