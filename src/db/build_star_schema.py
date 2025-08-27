import duckdb
from tqdm import tqdm

DB_PATH = "data/chicago_311.duckdb"

def build_star_schema():
    with duckdb.connect(DB_PATH) as con:
        con.execute("PRAGMA threads=8;")  # use all CPU threads
        con.execute("PRAGMA memory_limit='16GB';")  # optional: limit memory if needed

        # Drop existing tables
        tables = ["fact_requests", "dim_service", "dim_agency", "dim_location", "dim_time", "raw_requests_dedup", "fact_requests_ready"]
        for t in tables:
            con.execute(f"DROP TABLE IF EXISTS {t};")

        # Deduplicate raw requests
        print("⏳ Deduplicating raw.requests...")
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

        # Create dimension tables
        print("⏳ Creating dimension tables...")
        con.execute("""
        CREATE TABLE dim_service (
            service_id INT PRIMARY KEY,
            service_name VARCHAR
        );""")
        con.execute("""
        CREATE TABLE dim_agency (
            agency_id INT PRIMARY KEY,
            agency_name VARCHAR
        );""")
        con.execute("""
        CREATE TABLE dim_location (
            location_id INT PRIMARY KEY,
            street_number VARCHAR,
            street_name VARCHAR,
            street_type VARCHAR,
            street_direction VARCHAR,
            city VARCHAR,
            state VARCHAR,
            zip_code VARCHAR,
            community_area VARCHAR,
            ward VARCHAR,
            police_district VARCHAR,
            police_beat VARCHAR,
            latitude DOUBLE,
            longitude DOUBLE,
            location_key VARCHAR
        );""")
        con.execute("""
        CREATE TABLE dim_time (
            date_id DATE PRIMARY KEY,
            year INT,
            month INT,
            day_of_week INT,
            is_weekend BOOLEAN
        );""")
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
        );""")

        # Populate dimension tables
        print("⏳ Populating dim_service, dim_agency, dim_location, dim_time...")
        con.execute("""
        INSERT INTO dim_service(service_id, service_name)
        SELECT ROW_NUMBER() OVER(ORDER BY sr_type), sr_type
        FROM (SELECT DISTINCT sr_type FROM raw_requests_dedup) AS new_services;
        """)
        con.execute("""
        INSERT INTO dim_agency(agency_id, agency_name)
        SELECT ROW_NUMBER() OVER(ORDER BY owner_department), owner_department
        FROM (SELECT DISTINCT owner_department FROM raw_requests_dedup) AS new_agencies;
        """)
        con.execute("""
        INSERT INTO dim_location(location_id, street_number, street_name, street_type, street_direction,
                                 city, state, zip_code, community_area, ward, police_district,
                                 police_beat, latitude, longitude, location_key)
        SELECT ROW_NUMBER() OVER(ORDER BY street_number, street_name),
               street_number, street_name, street_type, street_direction,
               city, state, zip_code, community_area, ward,
               police_district, police_beat,
               ROUND(CAST(latitude AS DOUBLE), 6),
               ROUND(CAST(longitude AS DOUBLE), 6),
               md5(COALESCE(TRIM(street_number),'') || COALESCE(TRIM(street_name),'') || COALESCE(TRIM(zip_code),'')) AS location_key
        FROM (
            SELECT DISTINCT street_number, street_name, street_type, street_direction,
                            city, state, zip_code, community_area, ward,
                            police_district, police_beat, latitude, longitude
            FROM raw_requests_dedup
        ) AS loc;
        """)
        con.execute("""
        INSERT INTO dim_time(date_id, year, month, day_of_week, is_weekend)
        SELECT DISTINCT CAST(created_date AS DATE),
               EXTRACT(YEAR FROM CAST(created_date AS TIMESTAMP)),
               EXTRACT(MONTH FROM CAST(created_date AS TIMESTAMP)),
               EXTRACT(DOW FROM CAST(created_date AS TIMESTAMP)),
               CASE WHEN EXTRACT(DOW FROM CAST(created_date AS TIMESTAMP)) IN (0,6) THEN TRUE ELSE FALSE END
        FROM raw_requests_dedup;
        """)

        # Add location_key to raw_requests_dedup
        con.execute("""
        ALTER TABLE raw_requests_dedup ADD COLUMN IF NOT EXISTS location_key VARCHAR;
        UPDATE raw_requests_dedup
        SET location_key = md5(
            COALESCE(TRIM(street_number),'') || COALESCE(TRIM(street_name),'') || COALESCE(TRIM(zip_code),'')
        );
        """)

        # Step 1: Precompute all IDs for fact_requests
        print("⏳ Precomputing fact_requests IDs (lightweight join)...")
        con.execute("""
        CREATE TABLE fact_requests_ready AS
        SELECT r.sr_number, r.created_date, r.closed_date, r.status,
               COALESCE(s.service_id,-1) AS service_id,
               COALESCE(a.agency_id,-1) AS agency_id,
               COALESCE(l.location_id,-1) AS location_id,
               t.date_id
        FROM raw_requests_dedup r
        LEFT JOIN dim_service s ON TRIM(r.sr_type) = TRIM(s.service_name)
        LEFT JOIN dim_agency a ON TRIM(r.owner_department) = TRIM(a.agency_name)
        LEFT JOIN dim_location l ON r.location_key = l.location_key
        LEFT JOIN dim_time t ON CAST(r.created_date AS DATE) = t.date_id;
        """)

        # Step 2: Insert fact_requests in batches (memory-safe)
        print("⏳ Populating fact_requests in batches...")
        batch_size = 50_000
        total_rows = con.execute("SELECT COUNT(*) FROM fact_requests_ready").fetchone()[0]

        for offset in tqdm(range(0, total_rows, batch_size), desc="Batches"):
            con.execute(f"""
            INSERT INTO fact_requests(request_id, created_date, closed_date, status,
                                      closure_time, service_id, agency_id, location_id, date_id)
            SELECT 
                CONCAT(r.sr_number, '_', ROW_NUMBER() OVER(PARTITION BY r.sr_number ORDER BY r.created_date)) AS request_id,
                r.created_date,
                r.closed_date,
                r.status,
                EXTRACT(EPOCH FROM (r.closed_date - r.created_date))/3600 AS closure_time,
                r.service_id, r.agency_id, r.location_id, r.date_id
            FROM fact_requests_ready r
            LIMIT {batch_size} OFFSET {offset};
            """)
  
        print("✅ Star schema created successfully!")

if __name__ == "__main__":
    build_star_schema()
 