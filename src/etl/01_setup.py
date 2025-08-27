# src/etl/01_setup.py
import duckdb
import toml

# Load config
config = toml.load("conf/config.toml")
DB_PATH = config["db"]["path"]
DEBUG = config["etl"]["debug"]

def setup_raw_requests():
    with duckdb.connect(DB_PATH) as con:
        con.execute("PRAGMA threads=8;")
        con.execute("PRAGMA memory_limit='16GB';")

        tables = ["fact_requests", "dim_service", "dim_agency", "dim_location", 
                  "dim_time", "raw_requests_dedup", "fact_requests_ready"]
        for t in tables:
            con.execute(f"DROP TABLE IF EXISTS {t};")

        print("⏳ Deduplicating raw.requests...")
        if DEBUG:
            con.execute("CREATE TABLE raw_requests_dedup AS SELECT * FROM raw.requests LIMIT 10;")
        else:
            con.execute("""
            CREATE TABLE raw_requests_dedup AS
            SELECT *
            FROM (
                SELECT *, ROW_NUMBER() OVER(PARTITION BY sr_number ORDER BY created_date ASC) AS rn
                FROM raw.requests
            ) t
            WHERE rn = 1;
            """)
        print("✅ Deduplication complete.")

if __name__ == "__main__":
    setup_raw_requests()
