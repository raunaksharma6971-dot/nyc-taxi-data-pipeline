import duckdb

DB_PATH = "warehouse/nyc_taxi.duckdb"

def main():
    con = duckdb.connect(DB_PATH)

    checks = [
        ("table_not_empty", "SELECT COUNT(*) > 0 FROM taxi_trips"),
        ("pickup_not_null", "SELECT COUNT(*) = 0 FROM taxi_trips WHERE tpep_pickup_datetime IS NULL"),
        ("dropoff_not_null", "SELECT COUNT(*) = 0 FROM taxi_trips WHERE tpep_dropoff_datetime IS NULL"),
        ("non_negative_fare", "SELECT COUNT(*) = 0 FROM taxi_trips WHERE fare_amount < 0"),
        ("non_negative_distance", "SELECT COUNT(*) = 0 FROM taxi_trips WHERE trip_distance < 0"),
        ("passenger_count_reasonable", "SELECT COUNT(*) = 0 FROM taxi_trips WHERE passenger_count < 0 OR passenger_count > 8"),
        ("trip_time_positive", """
            SELECT COUNT(*) = 0
            FROM taxi_trips
            WHERE tpep_dropoff_datetime <= tpep_pickup_datetime
        """),
    ]

    failed = []
    for name, sql in checks:
        ok = con.execute(sql).fetchone()[0]
        print(("✅" if ok else "❌"), name)
        if not ok:
            failed.append(name)

    con.close()

    if failed:
        raise SystemExit(f"Quality checks failed: {failed}")

    print("✅ All quality checks passed")

if __name__ == "__main__":
    main()
