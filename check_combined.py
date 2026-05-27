import duckdb

conn = duckdb.connect("dagster_project/data/staging/data.duckdb")

print("=== Row counts by source ===")
print(conn.execute("""
    SELECT source, COUNT(*) as total_activities
    FROM combined_activities
    GROUP BY source
    ORDER BY source
""").df().to_string(index=False))

print("\n=== Date coverage by source ===")
print(conn.execute("""
    SELECT source, MIN(date) as earliest, MAX(date) as latest
    FROM combined_activities
    GROUP BY source
""").df().to_string(index=False))

print("\n=== Dates with entries from BOTH sources (should be 0) ===")
both = conn.execute("""
    SELECT date, COUNT(DISTINCT source) as source_count
    FROM combined_activities
    GROUP BY date
    HAVING COUNT(DISTINCT source) > 1
""").df()
print(f"  {len(both)} such dates found")
if len(both) > 0:
    print(both.head(10).to_string(index=False))

print("\n=== Overlap period sample Jun 2023 (expect garmin) ===")
print(conn.execute("""
    SELECT source, date, activity_type, title, distance_mi
    FROM combined_activities
    WHERE date BETWEEN '2023-06-01' AND '2023-06-30'
    ORDER BY date
    LIMIT 10
""").df().to_string(index=False))

print("\n=== Garmin gap Jul-Aug 2025 (expect strava) ===")
print(conn.execute("""
    SELECT source, date, activity_type, title, distance_mi
    FROM combined_activities
    WHERE date BETWEEN '2025-07-01' AND '2025-08-31'
    ORDER BY date
    LIMIT 10
""").df().to_string(index=False))

print("\n=== Post-Nov 2025 (expect strava) ===")
print(conn.execute("""
    SELECT source, date, activity_type, title, distance_mi
    FROM combined_activities
    WHERE date >= '2025-12-01'
    ORDER BY date
    LIMIT 10
""").df().to_string(index=False))
