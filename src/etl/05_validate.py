# src/etl/05_validate.py
import duckdb
import toml
import pandas as pd

config = toml.load("conf/config.toml")
DB_PATH = config["db"]["path"]

def validate_etl():
    with duckdb.connect(DB_PATH) as con:
        print("🔎 Validating ETL pipeline...\n")

        # --- Row counts ---
        print("📊 Row counts:")
        for table in ["raw_requests_dedup", "dim_service", "dim_agency", 
                      "dim_location", "dim_time", 
                      "fact_requests_ready", "fact_requests"]:
            try:
                count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                print(f"  {table:20} -> {count:,}")
            except Exception as e:
                print(f"  {table:20} -> ❌ {e}")

        print("\n")

        # --- Null checks for fact table ---
        print("❓ Null checks in fact_requests:")
        nulls = con.execute("""
        SELECT 
            SUM(CASE WHEN request_id IS NULL THEN 1 ELSE 0 END) AS null_request_id,
            SUM(CASE WHEN service_id IS NULL THEN 1 ELSE 0 END) AS null_service_id,
            SUM(CASE WHEN agency_id IS NULL THEN 1 ELSE 0 END) AS null_agency_id,
            SUM(CASE WHEN location_id IS NULL THEN 1 ELSE 0 END) AS null_location_id,
            SUM(CASE WHEN date_id IS NULL THEN 1 ELSE 0 END) AS null_date_id
        FROM fact_requests;
        """).fetchdf()
        print(nulls.to_string(index=False))
        print("\n")

        # --- Referential integrity ---
        print("🔗 Referential integrity checks:")
        checks = {
            "service_id": "dim_service",
            "agency_id": "dim_agency",
            "location_id": "dim_location",
            "date_id": "dim_time"
        }
        for col, dim in checks.items():
            missing = con.execute(f"""
            SELECT COUNT(*) 
            FROM fact_requests f 
            LEFT JOIN {dim} d ON f.{col} = d.{col if col!='date_id' else 'date_id'}
            WHERE d.{col if col!='date_id' else 'date_id'} IS NULL;
            """).fetchone()[0]
            print(f"  {col:12} -> {missing} missing refs")

        print("\n")

        # --- Sample rows ---
        print("📋 Sample fact_requests rows:")
        sample = con.execute("SELECT * FROM fact_requests LIMIT 5;").fetchdf()
        print(sample)

        print("\n✅ Validation complete.")

if __name__ == "__main__":
    validate_etl()
