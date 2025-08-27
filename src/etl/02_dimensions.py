# src/etl/02_dimensions.py
import duckdb
import toml

config = toml.load("conf/config.toml")
DB_PATH = config["db"]["path"]

def create_dimensions():
    with duckdb.connect(DB_PATH) as con:
        print("⏳ Creating dimension tables...")

        # Service
        con.execute("""
        CREATE TABLE dim_service (
            service_id INT PRIMARY KEY,
            service_name VARCHAR
        );
        """)
        con.execute("""
        INSERT INTO dim_service(service_id, service_name)
        SELECT ROW_NUMBER() OVER(ORDER BY sr_type), sr_type
        FROM (SELECT DISTINCT sr_type FROM raw_requests_dedup);
        """)

        # Agency
        con.execute("""
        CREATE TABLE dim_agency (
            agency_id INT PRIMARY KEY,
            agency_name VARCHAR
        );
        """)
        con.execute("""
        INSERT INTO dim_agency(agency_id, agency_name)
        SELECT ROW_NUMBER() OVER(ORDER BY owner_department), owner_department
        FROM (SELECT DISTINCT owner_department FROM raw_requests_dedup);
        """)

        # Location
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
        );
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
        );
        """)

        # Time
        con.execute("""
        CREATE TABLE dim_time (
            date_id DATE PRIMARY KEY,
            year INT,
            month INT,
            day_of_week INT,
            is_weekend BOOLEAN
        );
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

        print("✅ Dimensions created.")

if __name__ == "__main__":
    create_dimensions()
