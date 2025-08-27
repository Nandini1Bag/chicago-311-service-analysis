# src/etl/04_fact_insert.py
import duckdb
import toml
import pandas as pd
from tqdm import tqdm
from pathlib import Path

# Load config
config = toml.load("conf/config.toml")
DB_PATH = config["db"]["path"]
DEBUG = config["etl"]["debug"]
BATCH_SIZE = 5 if DEBUG else config["etl"]["batch_size"]

DUP_CSV = Path("data/duplicates_skipped.csv")

def insert_fact():
    with duckdb.connect(DB_PATH) as con:
        con.execute("PRAGMA threads=8;")
        con.execute("PRAGMA max_temp_directory_size='50GB';")

        print("⏳ Populating fact_requests...")

        # Drop old fact table
        con.execute("DROP TABLE IF EXISTS fact_requests;")
        con.execute("""
        CREATE TABLE fact_requests (
            request_id VARCHAR PRIMARY KEY,
            created_date TIMESTAMP,
            closed_date TIMESTAMP,
            status VARCHAR,
            closure_time DOUBLE,
            service_id INT,
            agency_id INT,
            location_id INT,
            date_id DATE
        );
        """)

        total_rows = con.execute("SELECT COUNT(*) FROM fact_requests_ready").fetchone()[0]

        # Initialize duplicate log
        if DUP_CSV.exists():
            DUP_CSV.unlink()

        for offset in tqdm(range(0, total_rows, BATCH_SIZE), desc="Batches"):
            try:
                # Fetch batch
                df_batch = con.execute(f"""
                    SELECT 
                        CONCAT(sr_number, '_', ROW_NUMBER() OVER(PARTITION BY sr_number ORDER BY created_date)) AS request_id,
                        created_date,
                        closed_date,
                        status,
                        CASE WHEN closed_date IS NOT NULL 
                            THEN EXTRACT(EPOCH FROM (closed_date - created_date))/3600
                            ELSE NULL
                        END AS closure_time,
                        service_id,
                        agency_id,
                        location_id,
                        date_id
                    FROM fact_requests_ready
                    LIMIT {BATCH_SIZE} OFFSET {offset}
                """).fetchdf()

                # Drop duplicates on request_id
                dup_rows = df_batch[df_batch.duplicated(subset=["request_id"], keep="first")]
                if not dup_rows.empty:
                    dup_rows.to_csv(DUP_CSV, mode="a", index=False, header=not DUP_CSV.exists())
                    df_batch = df_batch.drop_duplicates(subset=["request_id"], keep="first")
                    print(f"⚠️  Skipped {len(dup_rows)} duplicate rows in batch offset {offset}")

                # Insert remaining rows
                con.register("batch_tmp", df_batch)
                con.execute("""
                    INSERT INTO fact_requests
                    SELECT * FROM batch_tmp
                """)

            except Exception as e:
                print(f"❌ Error in batch offset {offset}: {e}")

        print("✅ fact_requests populated successfully.")

if __name__ == "__main__":
    insert_fact()
