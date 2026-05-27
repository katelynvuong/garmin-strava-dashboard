from pathlib import Path

_DAGSTER_PROJECT_ROOT = Path(__file__).parent.parent.parent.parent

GARMIN_ACTIVITY_FILE_PATH = str(_DAGSTER_PROJECT_ROOT / "data" / "raw" / "garmin_data.csv")
STRAVA_ACTIVITIES_FILE_PATH = str(_DAGSTER_PROJECT_ROOT / "data" / "raw" / "strava_activities.csv")

# START_DATE = 
# END_DATE = 