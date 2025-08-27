# src/etl/03_fact_prepare.py
import duckdb
import toml
from tqdm import tqdm

# Load configuration
config = toml.load("conf/config.toml")
DB_PATH = config["db"]["path"]
DEBUG = config["etl"]["debug"]
BATCH_SIZE = 100_000 if not DEBUG else 5

def prepare_fact():
    with duckdb.connect(DB_PATH) as con:
        con.execute("PRAGMA threads=8;")
        con.execute("PRAGMA max_temp_directory_size='50GB';")

        # Add location_key if missing
        print("⏳ Adding location_key if missing...")
        con.execute("ALTER TABLE raw_requests_dedup ADD COLUMN IF NOT EXISTS location_key VARCHAR;")
        con.execute("""
        UPDATE raw_requests_dedup
        SET location_key = md5(
            COALESCE(TRIM(street_number),'') || 
            COALESCE(TRIM(street_name),'') || 
            COALESCE(TRIM(zip_code),'')
        );
        """)

        # Drop old table and create new fact_requests_ready
        con.execute("DROP TABLE IF EXISTS fact_requests_ready;")
        con.execute("""
        CREATE TABLE fact_requests_ready (
            sr_number VARCHAR,
            created_date TIMESTAMP,
            closed_date TIMESTAMP,
            status VARCHAR,
            service_id INT,
            agency_id INT,
            location_id INT,
            date_id DATE
        );
        """)

        # Get list of months to process
        months = con.execute("""
            SELECT DISTINCT 
                   EXTRACT(YEAR FROM CAST(created_date AS TIMESTAMP)) AS year,
                   EXTRACT(MONTH FROM CAST(created_date AS TIMESTAMP)) AS month
            FROM raw_requests_dedup
            ORDER BY year, month;
        """).fetchall()

        print("⏳ Creating fact_requests_ready in batches by month...")
        for year, month in tqdm(months, desc="Months"):
            count = con.execute(f"""
                SELECT COUNT(*) 
                FROM raw_requests_dedup
                WHERE EXTRACT(YEAR FROM CAST(created_date AS TIMESTAMP))={int(year)}
                  AND EXTRACT(MONTH FROM CAST(created_date AS TIMESTAMP))={int(month)};
            """).fetchone()[0]
            if count == 0:
                continue

            # Insert month data in batches
            for offset in range(0, count, BATCH_SIZE):
                con.execute(f"""
                    INSERT INTO fact_requests_ready (
                        sr_number,
                        created_date,
                        closed_date,
                        status,
                        service_id,
                        agency_id,
                        location_id,
                        date_id
                    )
                    SELECT 
                        r.sr_number AS sr_number,
                        CAST(r.created_date AS TIMESTAMP) AS created_date,
                        CAST(r.closed_date AS TIMESTAMP) AS closed_date,
                        r.status AS status,
                        COALESCE(s.service_id,-1) AS service_id,
                        COALESCE(a.agency_id,-1) AS agency_id,
                        COALESCE(l.location_id,-1) AS location_id,
                        CAST(r.created_date AS DATE) AS date_id
                    FROM raw_requests_dedup r
                    LEFT JOIN dim_service s ON TRIM(r.sr_type) = TRIM(s.service_name)
                    LEFT JOIN dim_agency a ON TRIM(r.owner_department) = TRIM(a.agency_name)
                    LEFT JOIN dim_location l ON r.location_key = l.location_key
                    WHERE EXTRACT(YEAR FROM CAST(r.created_date AS TIMESTAMP))={int(year)}
                      AND EXTRACT(MONTH FROM CAST(r.created_date AS TIMESTAMP))={int(month)}
                    LIMIT {BATCH_SIZE} OFFSET {offset};
                """)

        print("✅ fact_requests_ready created successfully.")

if __name__ == "__main__":
    prepare_fact()
