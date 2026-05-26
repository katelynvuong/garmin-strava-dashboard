import os
import dagster as dg
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


@dg.asset
def strava_activities_file(context: dg.AssetExecutionContext) -> None:
    """
    The raw CSV file for the Strava activities dataset.
    """
    if not os.path.exists(constants.STRAVA_ACTIVITIES_FILE_PATH):
        raise FileNotFoundError(
            f"Strava activity CSV not found at: {constants.STRAVA_ACTIVITIES_FILE_PATH}"
        )

    context.log.info(f"Strava activity file found at {constants.STRAVA_ACTIVITIES_FILE_PATH}")


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

    with database.get_connection() as conn:
        conn.execute(query)


@dg.asset(
    deps=["strava_activities_file"]
)
def strava_activities(context: dg.AssetExecutionContext, database: DuckDBResource) -> None:

    query = f"""
        create or replace table strava_activities as (
          select
            "Activity Date"                                  as activity_date,
            "Activity Name"                                  as activity_name,
            "Activity Type"                                  as activity_type,
            "Activity Description"                           as activity_description,
            ROUND("Elapsed Time" / 60.0, 2)                 as elapsed_time_min,
            ROUND(CAST(REPLACE("Distance", ',', '') AS DOUBLE) / 1.609344, 2) as distance_mi,
            ROUND("Moving Time" / 60.0, 2)                  as moving_time_min,
            ROUND("Average Speed" * 2.23694, 2)             as avg_speed_mph,
            "Total Steps"                                    as total_steps
          from '{constants.STRAVA_ACTIVITIES_FILE_PATH}'
        );
    """

    with database.get_connection() as conn:
        conn.execute(query)
