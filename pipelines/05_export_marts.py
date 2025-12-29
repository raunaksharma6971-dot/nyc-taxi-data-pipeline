import os
import duckdb
import pandas as pd
from pathlib import Path

DB_PATH = Path("warehouse/nyc_taxi.duckdb")
OUT_DIR = Path("outputs")
OUT_DIR.mkdir(parents=True, exist_ok=True)

def export_table(con, table_name: str):
    df = con.execute(f"SELECT * FROM {table_name}").df()
    out_csv = OUT_DIR / f"{table_name}.csv"
    df.to_csv(out_csv, index=False)
    print(f"Exported {table_name} -> {out_csv} ({len(df):,} rows)")

def main():
    con = duckdb.connect(str(DB_PATH))

    export_table(con, "mart_trips_by_borough")
    export_table(con, "mart_trips_by_hour")

    con.close()
    print("All exports complete")

if __name__ == "__main__":
    main()
