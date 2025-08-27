import duckdb
from tqdm import tqdm

DB_PATH = "data/chicago_311.duckdb"

def populate_fact_requests_with_fallback():
    with duckdb.connect(DB_PATH) as con:
        con.execute("PRAGMA threads=8;")  # optimize CPU usage

        batch_size = 500_000
        total_rows = con.execute("SELECT COUNT(*) FROM raw_requests_dedup").fetchone()[0]
        print(f"Total rows in deduplicated raw table: {total_rows}")

        for start in tqdm(range(0, total_rows, batch_size), desc="Batches"):
            # Use ROW_NUMBER for batching
            batch_df = con.execute(f"""
            WITH numbered AS (
                SELECT *, ROW_NUMBER() OVER () AS rn
                FROM raw_requests_dedup
            )
            SELECT *
            FROM numbered
            WHERE rn BETWEEN {start+1} AND {start + batch_size};
            """).fetchdf()

            if batch_df.empty:
                continue

            # Create temporary table for the batch
            con.execute("CREATE TEMPORARY TABLE temp_batch AS SELECT * FROM batch_df")

            # Insert into fact_requests with fallback IDs
            con.execute(f"""
            INSERT INTO fact_requests(request_id, created_date, closed_date, status,
                                      closure_time, service_id, agency_id, location_id, date_id)
            SELECT 
                CONCAT(r.sr_number, '_', ROW_NUMBER() OVER(PARTITION BY r.sr_number ORDER BY r.created_date)) AS request_id,
                CAST(r.created_date AS TIMESTAMP),
                CAST(r.closed_date AS TIMESTAMP),
                r.status,
                EXTRACT(EPOCH FROM (CAST(r.closed_date AS TIMESTAMP) - CAST(r.created_date AS TIMESTAMP)))/3600 AS closure_time,
                COALESCE(s.service_id, -1) AS service_id,
                COALESCE(a.agency_id, -1) AS agency_id,
                COALESCE(l.location_id, -1) AS location_id,
                t.date_id
            FROM temp_batch r
            LEFT JOIN dim_service s ON TRIM(r.sr_type) = TRIM(s.service_name)
            LEFT JOIN dim_agency a ON TRIM(r.owner_department) = TRIM(a.agency_name)
            LEFT JOIN dim_location l
                   ON TRIM(r.street_number) = TRIM(l.street_number)
                  AND TRIM(r.street_name) = TRIM(l.street_name)
                  AND TRIM(r.street_type) = TRIM(l.street_type)
                  AND TRIM(r.street_direction) = TRIM(l.street_direction)
                  AND TRIM(r.city) = TRIM(l.city)
                  AND TRIM(r.state) = TRIM(l.state)
                  AND TRIM(r.zip_code) = TRIM(l.zip_code)
                  AND TRIM(r.community_area) = TRIM(l.community_area)
                  AND TRIM(r.ward) = TRIM(l.ward)
                  AND TRIM(r.police_district) = TRIM(l.police_district)
                  AND TRIM(r.police_beat) = TRIM(l.police_beat)
                  AND (r.latitude IS NOT NULL AND ROUND(CAST(r.latitude AS DOUBLE),6) = l.latitude)
                  AND (r.longitude IS NOT NULL AND ROUND(CAST(r.longitude AS DOUBLE),6) = l.longitude)
            LEFT JOIN dim_time t ON CAST(r.created_date AS DATE) = t.date_id;
            """)

        print("✅ fact_requests populated with fallback IDs successfully!")

if __name__ == "__main__":
    populate_fact_requests_with_fallback()
