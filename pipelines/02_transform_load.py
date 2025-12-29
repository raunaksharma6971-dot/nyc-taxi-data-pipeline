import duckdb
import pandas as pd
import os

PROCESSED_DIR = "data/processed"
WAREHOUSE_DIR = "warehouse"
os.makedirs(WAREHOUSE_DIR, exist_ok=True)

DB_PATH = f"{WAREHOUSE_DIR}/nyc_taxi.duckdb"

def main():
    print("Reading extracted data...")
    trips = pd.read_parquet(f"{PROCESSED_DIR}/yellow_q1_2025_extracted.parquet")

    print("Reading zone lookup...")
    zones = pd.read_csv("data/raw/taxi_zone_lookup.csv")

    # ----------------------------
    # 1) Transform columns
    # ----------------------------
    print("Transforming data...")

    # Parse datetimes safely
    trips["tpep_pickup_datetime"] = pd.to_datetime(trips["tpep_pickup_datetime"], errors="coerce")
    trips["tpep_dropoff_datetime"] = pd.to_datetime(trips["tpep_dropoff_datetime"], errors="coerce")

    # Create easy-to-use time fields
    trips["pickup_date"] = trips["tpep_pickup_datetime"].dt.date
    trips["pickup_hour"] = trips["tpep_pickup_datetime"].dt.hour

    # Trip duration in minutes
    trips["trip_minutes"] = (
        (trips["tpep_dropoff_datetime"] - trips["tpep_pickup_datetime"])
        .dt.total_seconds() / 60
    )

    # ----------------------------
    # 2) Join zone lookup (enrichment)
    # ----------------------------
    print("Joining zone lookup...")
    trips = trips.merge(
        zones[["LocationID", "Zone", "Borough"]],
        left_on="PULocationID",
        right_on="LocationID",
        how="left"
    ).drop(columns=["LocationID"])

    # Rename to make it clear this is pickup location info
    trips = trips.rename(columns={"Zone": "pickup_zone", "Borough": "Borough"})

    # ----------------------------
    # 3) Clean bad rows (so quality checks pass)
    # ----------------------------
    print("Cleaning bad rows...")

    before = len(trips)

    # Remove rows with missing timestamps
    trips = trips.dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime"])

    # Ensure dropoff is after pickup
    trips = trips[trips["tpep_dropoff_datetime"] > trips["tpep_pickup_datetime"]]

    # Remove negative fares/distances
    trips = trips[trips["fare_amount"].fillna(0) >= 0]
    trips = trips[trips["trip_distance"].fillna(0) >= 0]

    # Passenger count sanity (allow 0–8)
    trips = trips[trips["passenger_count"].fillna(0).between(0, 8)]

    # Trip duration sanity (1 to 300 minutes)
    trips = trips[trips["trip_minutes"].between(1, 300)]

    after = len(trips)
    removed = before - after
    print(f"Removed {removed:,} bad rows ({removed / before:.2%})")
    print(f"Clean rows remaining: {after:,}")

    # ----------------------------
    # 4) Load into DuckDB
    # ----------------------------
    print("Loading into DuckDB...")
    con = duckdb.connect(DB_PATH)

    con.execute("DROP TABLE IF EXISTS taxi_trips")
    con.execute("CREATE TABLE taxi_trips AS SELECT * FROM trips")

    row_count = con.execute("SELECT COUNT(*) FROM taxi_trips").fetchone()[0]
    print(f"Loaded {row_count:,} rows into DuckDB at {DB_PATH}")

    con.close()
    print("Done.")

if __name__ == "__main__":
    main()
