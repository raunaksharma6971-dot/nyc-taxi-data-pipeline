import glob
import os
import pandas as pd

RAW_DIR = "data/raw"
OUT_DIR = "data/processed"
os.makedirs(OUT_DIR, exist_ok=True)

def main():
    files = sorted(glob.glob(f"{RAW_DIR}/yellow_tripdata_2025-*.parquet"))
    if not files:
        raise FileNotFoundError("No parquet files found in data/raw")

    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

    # Keep only important columns to reduce size + improve performance
    keep_cols = [
        "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "PULocationID", "DOLocationID",
        "passenger_count", "trip_distance",
        "fare_amount", "total_amount", "payment_type"
    ]
    df = df[[c for c in keep_cols if c in df.columns]]

    out_path = f"{OUT_DIR}/yellow_q1_2025_extracted.parquet"
    df.to_parquet(out_path, index=False)
    print("Saved:", out_path, "| rows:", len(df), "| cols:", df.shape[1])

if __name__ == "__main__":
    main()
