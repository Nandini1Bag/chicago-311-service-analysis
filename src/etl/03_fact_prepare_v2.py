# src/etl/03_fact_prepare_v3.py
import duckdb
import toml
from tqdm import tqdm

# Load configuration
config = toml.load("conf/config.toml")
DB_PATH = config["db"]["path"]
DEBUG = config["etl"]["debug"]
BATCH_SIZE = 100_000 if not DEBUG else 5

def prepare_fact_v3():
    with duckdb.connect(DB_PATH) as con:
        con.execute("PRAGMA threads=8;")
        con.execute("PRAGMA max_temp_directory_size='50GB';")

        # --- Ensure unique dimensions before join ---
        print("⏳ Deduplicating dimension tables...")
        con.execute("""
        CREATE OR REPLACE TEMP TABLE dim_service_v3_unique AS
        SELECT service_name, MIN(service_id) AS service_id
        FROM dim_service_v3
        GROUP BY service_name;
        """)
        con.execute("""
        CREATE OR REPLACE TEMP TABLE dim_department_v3_unique AS
        SELECT department_name, MIN(department_id) AS department_id
        FROM dim_department_v3
        GROUP BY department_name;
        """)
        con.execute("""
        CREATE OR REPLACE TEMP TABLE dim_location_v3_unique AS
        SELECT location_key, MIN(location_id) AS location_id
        FROM dim_location_v3
        GROUP BY location_key;
        """)

        # --- Add location_key if missing ---
        print("⏳ Adding location_key if missing...")
        con.execute("ALTER TABLE raw_requests_dedup_v2 ADD COLUMN IF NOT EXISTS location_key VARCHAR;")
        con.execute("""
        UPDATE raw_requests_dedup_v2
        SET location_key = md5(
            COALESCE(TRIM(street_number),'') || 
            COALESCE(TRIM(street_name),'') || 
            COALESCE(TRIM(zip_code),'')
        );
        """)

        # --- Drop old fact table and create new ---
        con.execute("DROP TABLE IF EXISTS fact_requests_v3;")
        con.execute("""
        CREATE TABLE fact_requests_v3 (
            fact_id BIGINT,
            sr_number VARCHAR,
            created_date TIMESTAMP,
            closed_date TIMESTAMP,
            status VARCHAR,
            service_id INT,
            department_id INT,
            location_id INT,
            date_id DATE,
            request_count INT DEFAULT 1,
            created_at TIMESTAMP DEFAULT current_timestamp
        );
        """)

        # --- Process data month by month ---
        months = con.execute("""
            SELECT DISTINCT 
                   EXTRACT(YEAR FROM CAST(created_date AS TIMESTAMP)) AS year,
                   EXTRACT(MONTH FROM CAST(created_date AS TIMESTAMP)) AS month
            FROM raw_requests_dedup_v2
            ORDER BY year, month;
        """).fetchall()

        print("⏳ Creating fact_requests_v3 in batches by month...")
        for year, month in tqdm(months, desc="Months"):
            count = con.execute(f"""
                SELECT COUNT(*) 
                FROM raw_requests_dedup_v2
                WHERE EXTRACT(YEAR FROM CAST(created_date AS TIMESTAMP))={int(year)}
                  AND EXTRACT(MONTH FROM CAST(created_date AS TIMESTAMP))={int(month)};
            """).fetchone()[0]
            if count == 0:
                continue

            # Insert in batches
            for offset in range(0, count, BATCH_SIZE):
                con.execute(f"""
                    INSERT INTO fact_requests_v3 (
                        sr_number,
                        created_date,
                        closed_date,
                        status,
                        service_id,
                        department_id,
                        location_id,
                        date_id
                    )
                    SELECT 
                        r.sr_number,
                        CAST(r.created_date AS TIMESTAMP),
                        CAST(r.closed_date AS TIMESTAMP),
                        r.status,
                        COALESCE(s.service_id,-1),
                        COALESCE(d.department_id,-1),
                        COALESCE(l.location_id,-1),
                        CAST(r.created_date AS DATE)
                    FROM raw_requests_dedup_v2 r
                    LEFT JOIN dim_service_v3_unique s ON TRIM(r.sr_type) = TRIM(s.service_name)
                    LEFT JOIN dim_department_v3_unique d ON TRIM(r.created_department) = TRIM(d.department_name)
                    LEFT JOIN dim_location_v3_unique l ON r.location_key = l.location_key
                    WHERE EXTRACT(YEAR FROM CAST(r.created_date AS TIMESTAMP))={int(year)}
                      AND EXTRACT(MONTH FROM CAST(r.created_date AS TIMESTAMP))={int(month)}
                    LIMIT {BATCH_SIZE} OFFSET {offset};
                """)

        # --- Assign sequential fact_id ---
        print("⏳ Assigning sequential fact_id...")
        con.execute("""
            CREATE TABLE fact_requests_v3_new AS
            SELECT
                ROW_NUMBER() OVER () AS fact_id,
                sr_number,
                created_date,
                closed_date,
                status,
                service_id,
                department_id,
                location_id,
                date_id,
                request_count,
                created_at
            FROM fact_requests_v3;
        """)
        con.execute("DROP TABLE fact_requests_v3;")
        con.execute("ALTER TABLE fact_requests_v3_new RENAME TO fact_requests_v3;")

        print("✅ fact_requests_v3 created successfully without duplicates.")

if __name__ == "__main__":
    prepare_fact_v3()
