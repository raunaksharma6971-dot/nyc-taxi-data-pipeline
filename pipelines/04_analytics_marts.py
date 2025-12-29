import duckdb
from pathlib import Path

# Path to DuckDB warehouse
DB_PATH = Path("warehouse/nyc_taxi.duckdb")

def main():
    con = duckdb.connect(str(DB_PATH))

    print("Creating analytics marts...")

    # --------------------------------------------------
    # MART 1: Trips by Borough (high-level demand view)
    # --------------------------------------------------
    con.execute("""
    CREATE OR REPLACE TABLE mart_trips_by_borough AS
    SELECT
        Borough,
        COUNT(*) AS total_trips,
        ROUND(AVG(trip_distance), 2) AS avg_trip_distance,
        ROUND(AVG(total_amount), 2) AS avg_fare_amount
    FROM taxi_trips
    WHERE Borough IS NOT NULL
    GROUP BY Borough
    ORDER BY total_trips DESC;
    """)

    print("✓ mart_trips_by_borough created")

    # --------------------------------------------------
    # MART 2: Trips by Hour of Day (demand pattern)
    # --------------------------------------------------
    con.execute("""
    CREATE OR REPLACE TABLE mart_trips_by_hour AS
    SELECT
        EXTRACT(hour FROM tpep_pickup_datetime) AS pickup_hour,
        COUNT(*) AS total_trips,
        ROUND(AVG(trip_distance), 2) AS avg_trip_distance,
        ROUND(AVG(total_amount), 2) AS avg_fare_amount
    FROM taxi_trips
    WHERE tpep_pickup_datetime IS NOT NULL
    GROUP BY EXTRACT(hour FROM tpep_pickup_datetime)
    ORDER BY pickup_hour;
    """)

    print("✓ mart_trips_by_hour created")

    con.close()
    print("✅ Analytics marts created successfully")

if __name__ == "__main__":
    main()
