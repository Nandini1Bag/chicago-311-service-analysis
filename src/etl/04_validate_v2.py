# src/etl/05_validate_v3.py
import duckdb
import toml
import pandas as pd

# Load config
try:
    config = toml.load("conf/config.toml")
    DB_PATH = config.get("db", {}).get("path", "data/chicago_311.duckdb")
except Exception as e:
    print(f"⚠️ Could not load config.toml: {e}")
    DB_PATH = "data/chicago_311.duckdb"

def validate_v3_etl():
    """
    Full validation for _v3 ETL tables
    """
    tables = [
        "dim_service_v3",
        "dim_department_v3",
        "dim_location_v3",
        "dim_time_v3",
        "dim_geography_v3",
        "dim_infrastructure_v3",
        "fact_requests_v3"
    ]

    fact_table = "fact_requests_v3"

    with duckdb.connect(DB_PATH) as con:
        print("🔎 Validating ETL pipeline (_v3 tables)\n")

        # --- Row counts for all tables ---
        print("📊 Row counts:")
        table_counts = {}
        for table in tables:
            try:
                count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                table_counts[table] = count
                print(f"  {table:25} -> {count:,}")
            except Exception as e:
                print(f"  {table:25} -> ❌ {e}")
        print("\n")

        # --- Row count comparison with raw.requests ---
        print("🔄 Row count comparison with raw.requests:")
        try:
            raw_count = con.execute("SELECT COUNT(*) FROM raw.requests").fetchone()[0]
            fact_count = table_counts.get(fact_table, 0)
            dropped = raw_count - fact_count
            print(f"  Raw rows       : {raw_count:,}")
            print(f"  Fact rows      : {fact_count:,}")
            print(f"  Dropped rows   : {dropped:,} ({dropped/raw_count:.2%})")
        except Exception as e:
            print(f"  ❌ Could not fetch raw data count: {e}")
        print("\n")

        # --- Null checks for fact_requests_v3 ---
        print(f"❓ Null checks in {fact_table}:")
        try:
            nulls = con.execute(f"""
            SELECT 
                SUM(CASE WHEN fact_id IS NULL THEN 1 ELSE 0 END) AS null_fact_id,
                SUM(CASE WHEN service_id IS NULL THEN 1 ELSE 0 END) AS null_service_id,
                SUM(CASE WHEN department_id IS NULL THEN 1 ELSE 0 END) AS null_department_id,
                SUM(CASE WHEN location_id IS NULL THEN 1 ELSE 0 END) AS null_location_id,
                SUM(CASE WHEN date_id IS NULL THEN 1 ELSE 0 END) AS null_date_id
            FROM {fact_table};
            """).fetchdf()
            print(nulls.to_string(index=False))
        except Exception as e:
            print(f"  ❌ Could not check nulls: {e}")
        print("\n")

        # --- Referential integrity checks ---
        print("🔗 Referential integrity checks:")
        checks = {
            "service_id": "dim_service_v3",
            "department_id": "dim_department_v3",
            "location_id": "dim_location_v3",
            "date_id": "dim_time_v3"
        }
        for col, dim in checks.items():
            try:
                missing = con.execute(f"""
                SELECT COUNT(*) 
                FROM {fact_table} f
                LEFT JOIN {dim} d ON f.{col} = d.{col}
                WHERE d.{col} IS NULL;
                """).fetchone()[0]
                print(f"  {col:20} -> {missing} missing refs")
            except Exception as e:
                print(f"  {col:20} -> ❌ {e}")
        print("\n")

        # --- Sample rows from fact_requests_v3 ---
        print(f"📋 Sample rows from {fact_table}:")
        try:
            sample = con.execute(f"SELECT * FROM {fact_table} LIMIT 5;").fetchdf()
            print(sample)
        except Exception as e:
            print(f"  ❌ Could not fetch sample rows: {e}")
        print("\n✅ _v3 ETL validation complete.")

if __name__ == "__main__":
    validate_v3_etl()
