import os
import duckdb
import dagster as dg
from dagster._utils.backoff import backoff
from dagster_duckdb import DuckDBResource
from dagster_project.src.defs.assets import constants


@dg.asset
def garmin_activities_file(context: dg.AssetExecutionContext) -> None:
    """
    The raw CSV file for the Garmin activities dataset.
    """
    if not os.path.exists(constants.GARMIN_ACTIVITY_FILE_PATH):
        raise FileNotFoundError(
            f"Garmin activity CSV not found at: {constants.GARMIN_ACTIVITY_FILE_PATH}"
        )

    context.log.info(f"Garmin activity file found at {constants.GARMIN_ACTIVITY_FILE_PATH}")


@dg.asset(
    deps=["garmin_activities_file"]
)
def garmin_activities(context: dg.AssetExecutionContext, database: DuckDBResource) -> None:

    query = f"""
        create or replace table garmin_activities as (
          select
            "Activity Type"  as activity_type,
            "Date"           as date,
            "Title"          as title,
            "Distance"       as distance,
            "Calories"       as calories,
            "Time"           as time,
            "Avg HR"         as avg_hr,
            "Max HR"         as max_hr,
            "Avg Speed"      as avg_speed,
            "Steps"          as steps,
            "Moving Time"    as moving_time,
            "Elapsed Time"   as elapsed_time,
            "Min Elevation"  as min_elevation,
            "Max Elevation"  as max_elevation
          from '{constants.GARMIN_ACTIVITY_FILE_PATH}'
        );
    """

    conn = backoff(
        fn=duckdb.connect,
        retry_on=(RuntimeError, duckdb.IOException),
        kwargs={
            "database": os.getenv("DUCKDB_DATABASE"),
        },
        max_retries=10,
    )
    conn.execute(query)
