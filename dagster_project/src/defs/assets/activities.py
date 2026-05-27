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
            -- Strava stores swim distance in meters, all others in km
            ROUND(CASE
                WHEN "Activity Type" LIKE '%wim%'
                THEN CAST(REPLACE("Distance", ',', '') AS DOUBLE) / 1609.344
                ELSE CAST(REPLACE("Distance", ',', '') AS DOUBLE) / 1.609344
            END, 2)                                                  as distance_mi,
            ROUND("Moving Time" / 60.0, 2)                  as moving_time_min,
            ROUND("Average Speed" * 2.23694, 2)             as avg_speed_mph,
            "Total Steps"                                    as total_steps
          from '{constants.STRAVA_ACTIVITIES_FILE_PATH}'
        );
    """

    with database.get_connection() as conn:
        conn.execute(query)


@dg.asset(
    deps=["garmin_activities", "strava_activities"]
)
def combined_activities(database: DuckDBResource) -> None:
    """
    Unified activity table merging Garmin and Strava.

    Garmin is preferred for any day it recorded data. Strava fills in
    days with no Garmin entries (gaps and post-Nov 2025 data).

    Normalizes both sources to a common schema:
      - distance_mi        (miles)
      - elapsed/moving time in minutes
      - avg_speed_mph      (converted from Garmin's min/mile pace)
      - steps              (integer)
    """
    query = """
        CREATE OR REPLACE TABLE combined_activities AS
        WITH garmin_normalized AS (
            SELECT
                'garmin'                                                AS source,
                CAST(date AS DATE)                                      AS date,
                title,
                activity_type,
                -- Garmin swim distance is in yards, all others in miles
                CASE
                    WHEN LOWER(activity_type) LIKE '%swim%'
                    THEN ROUND(TRY_CAST(REPLACE(CAST(distance AS VARCHAR), ',', '') AS DOUBLE) / 1760.0, 4)
                    ELSE TRY_CAST(REPLACE(CAST(distance AS VARCHAR), ',', '') AS DOUBLE)
                END                                                     AS distance_mi,
                CASE
                    WHEN elapsed_time IS NULL OR elapsed_time IN ('--', '', '--:--:--')
                         OR elapsed_time LIKE '--%' THEN NULL
                    ELSE ROUND(
                        TRY_CAST(SPLIT_PART(elapsed_time, ':', 1) AS DOUBLE) * 60 +
                        TRY_CAST(SPLIT_PART(elapsed_time, ':', 2) AS DOUBLE) +
                        TRY_CAST(SPLIT_PART(SPLIT_PART(elapsed_time, ':', 3), '.', 1) AS DOUBLE) / 60, 2)
                END                                                     AS elapsed_time_min,
                CASE
                    WHEN moving_time IS NULL OR moving_time IN ('--', '', '--:--:--')
                         OR moving_time LIKE '--%' THEN NULL
                    ELSE ROUND(
                        TRY_CAST(SPLIT_PART(moving_time, ':', 1) AS DOUBLE) * 60 +
                        TRY_CAST(SPLIT_PART(moving_time, ':', 2) AS DOUBLE) +
                        TRY_CAST(SPLIT_PART(SPLIT_PART(moving_time, ':', 3), '.', 1) AS DOUBLE) / 60, 2)
                END                                                     AS moving_time_min,
                -- Garmin avg_speed is pace (MM:SS min/mile) -> convert to mph
                CASE
                    WHEN avg_speed IS NULL OR avg_speed IN ('--', '')
                         OR POSITION(':' IN avg_speed) = 0
                         OR avg_speed LIKE '--%' THEN NULL
                    ELSE ROUND(60.0 / (
                        TRY_CAST(SPLIT_PART(avg_speed, ':', 1) AS DOUBLE) +
                        TRY_CAST(SPLIT_PART(avg_speed, ':', 2) AS DOUBLE) / 60
                    ), 2)
                END                                                     AS avg_speed_mph,
                TRY_CAST(REPLACE(CAST(steps AS VARCHAR), ',', '')
                    AS INTEGER)                                         AS steps
            FROM garmin_activities
        ),
        strava_normalized AS (
            SELECT
                'strava'                                                AS source,
                -- Pad single-digit days ("Apr 8,") -> "Apr 08," before parsing
                strptime(
                    regexp_replace(activity_date, '^([A-Za-z]+) ([0-9]),', '\\1 0\\2,'),
                    '%b %d, %Y, %I:%M:%S %p'
                )::DATE                                                 AS date,
                activity_name                                           AS title,
                activity_type,
                distance_mi,
                elapsed_time_min,
                moving_time_min,
                avg_speed_mph,
                TRY_CAST(total_steps AS INTEGER)                        AS steps
            FROM strava_activities
        ),
        garmin_dates AS (
            SELECT DISTINCT date FROM garmin_normalized
        )
        SELECT source, date, title, activity_type,
               distance_mi, elapsed_time_min, moving_time_min, avg_speed_mph, steps
        FROM garmin_normalized

        UNION ALL

        -- Strava fills in only days with no Garmin data at all
        SELECT source, date, title, activity_type,
               distance_mi, elapsed_time_min, moving_time_min, avg_speed_mph, steps
        FROM strava_normalized
        WHERE date NOT IN (SELECT date FROM garmin_dates)

        ORDER BY date;
    """

    with database.get_connection() as conn:
        conn.execute(query)
