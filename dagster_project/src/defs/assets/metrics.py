import dagster as dg
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
from dagster_duckdb import DuckDBResource
from dagster_project.src.defs.assets import constants

RUNNING_TYPES = ("Running", "Run", "Treadmill Running")


@dg.asset(
    deps=["combined_activities"]
)
def monthly_mileage(database: DuckDBResource) -> dg.MaterializeResult:
    """
    Monthly running mileage across all historical records (Garmin + Strava).
    Saved as a PNG and surfaced as a plot preview in the Dagster UI.
    """
    with database.get_connection() as conn:
        df = conn.execute(f"""
            SELECT
                DATE_TRUNC('month', date)    AS month,
                SUM(distance_mi)             AS total_miles
            FROM combined_activities
            WHERE activity_type IN {RUNNING_TYPES}
              AND distance_mi IS NOT NULL
              AND distance_mi > 0
            GROUP BY 1
            ORDER BY 1
        """).df()

    df["month"] = pd.to_datetime(df["month"])

    fig, ax = plt.subplots(figsize=(16, 5))
    ax.bar(df["month"], df["total_miles"], width=25, color="#E05C2A", edgecolor="none")

    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.xaxis.set_minor_locator(mdates.MonthLocator())

    ax.set_xlabel("Month")
    ax.set_ylabel("Miles")
    ax.set_title("Monthly Running Mileage (All Time)")
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    plt.tight_layout()

    output_path = constants.OUTPUT_FILE_PATH.format("monthly_mileage")
    fig.savefig(output_path, dpi=150)
    plt.close(fig)

    return dg.MaterializeResult(
        metadata={
            "total_months": int(len(df)),
            "peak_month": str(df.loc[df["total_miles"].idxmax(), "month"].date()),
            "peak_miles": float(round(df["total_miles"].max(), 1)),
            "plot": dg.MetadataValue.path(output_path),
        }
    )
