# src/etl/01_setup_v2.py
import duckdb
import os

# Try different config loading methods
try:
    import tomllib  # Python 3.11+
    with open("conf/config.toml", "rb") as f:
        config = tomllib.load(f)
        print("✅ Config loaded using tomllib")
except ImportError:
    try:
        import toml  # fallback to toml package
        config = toml.load("conf/config.toml")
        print("✅ Config loaded using toml package")
    except ImportError:
        # Fallback to hardcoded values if no toml support
        print("⚠️  No TOML support found, using your config values")
        config = {
            "db": {"path": "data/chicago_311.duckdb"},
            "etl": {"debug": False}
        }
except FileNotFoundError:
    print("⚠️  Config file not found, using default values")
    config = {
        "db": {"path": "data/chicago_311.duckdb"},
        "etl": {"debug": False}
    }

DB_PATH = config["db"]["path"]
DEBUG = config["etl"]["debug"]

def setup_raw_requests_v2():
    """
    Parallel ETL setup using _v2 suffix to avoid disturbing existing tables
    """
    with duckdb.connect(DB_PATH) as con:
        con.execute("PRAGMA threads=8;")
        con.execute("PRAGMA memory_limit='16GB';")

        # V2 table list - parallel to existing tables
        tables_v2 = [
            "fact_requests_v2", 
            "dim_service_v2", 
            "dim_department_v2",  # Updated from dim_agency
            "dim_location_v2", 
            "dim_time_v2", 
            "dim_geography_v2",   # NEW - for community areas, wards, police districts
            "dim_infrastructure_v2",  # NEW - for electrical districts, utilities
            "dim_coordinates_v2", # NEW - for x/y coordinates, lat/long
            "raw_requests_dedup_v2", 
            "fact_requests_ready_v2"
        ]
        
        print("🚀 Starting parallel ETL setup (V2 tables)...")
        print(f"📋 Tables to be created: {', '.join(tables_v2)}")
        
        # Only drop V2 tables (preserves your original data)
        for t in tables_v2:
            con.execute(f"DROP TABLE IF EXISTS {t};")
            print(f"   Dropped existing {t} (if existed)")

        print("\n⏳ Deduplicating raw.requests -> raw_requests_dedup_v2...")
        if DEBUG:
            con.execute("CREATE TABLE raw_requests_dedup_v2 AS SELECT * FROM raw.requests LIMIT 10;")
            print("   🧪 DEBUG MODE: Using 10 sample records")
        else:
            con.execute("""
            CREATE TABLE raw_requests_dedup_v2 AS
            SELECT *
            FROM (
                SELECT *, 
                       ROW_NUMBER() OVER(PARTITION BY sr_number ORDER BY created_date ASC) AS rn
                FROM raw.requests
            ) t
            WHERE rn = 1;
            """)
            print("   📊 FULL MODE: Processing all records")
        
        record_count = con.execute("SELECT COUNT(*) FROM raw_requests_dedup_v2;").fetchone()[0]
        print(f"✅ Deduplication complete. Records: {record_count:,}")
        
        # Data quality checks
        print("\n⏳ Running comprehensive data quality checks...")
        
        # Critical fields check
        critical_fields_check = con.execute("""
        SELECT 
            COUNT(*) as total_records,
            SUM(CASE WHEN sr_number IS NULL OR TRIM(sr_number) = '' THEN 1 ELSE 0 END) as missing_sr_number,
            SUM(CASE WHEN created_date IS NULL THEN 1 ELSE 0 END) as missing_created_date,
            SUM(CASE WHEN sr_type IS NULL OR TRIM(sr_type) = '' THEN 1 ELSE 0 END) as missing_sr_type,
            SUM(CASE WHEN status IS NULL OR TRIM(status) = '' THEN 1 ELSE 0 END) as missing_status,
            SUM(CASE WHEN created_department IS NULL OR TRIM(created_department) = '' THEN 1 ELSE 0 END) as missing_created_dept
        FROM raw_requests_dedup_v2;
        """).fetchone()
        
        print(f"\n📊 Data Quality Summary:")
        print(f"   Total records: {critical_fields_check[0]:,}")
        print(f"   Missing SR numbers: {critical_fields_check[1]:,}")
        print(f"   Missing created dates: {critical_fields_check[2]:,}")
        print(f"   Missing service types: {critical_fields_check[3]:,}")
        print(f"   Missing status: {critical_fields_check[4]:,}")
        print(f"   Missing created department: {critical_fields_check[5]:,}")
        
        # Dimension cardinality analysis
        print("\n⏳ Analyzing dimension cardinalities...")
        
        dimension_analysis = con.execute("""
        SELECT 
            -- Service dimension
            COUNT(DISTINCT COALESCE(NULLIF(TRIM(sr_type), ''), 'Unknown')) as unique_service_types,
            COUNT(DISTINCT COALESCE(NULLIF(TRIM(sr_short_code), ''), 'Unknown')) as unique_short_codes,
            COUNT(DISTINCT COALESCE(NULLIF(TRIM(origin), ''), 'Unknown')) as unique_origins,
            
            -- Department dimension  
            COUNT(DISTINCT COALESCE(NULLIF(TRIM(created_department), ''), 'Unknown')) as unique_created_depts,
            COUNT(DISTINCT COALESCE(NULLIF(TRIM(owner_department), ''), 'Unknown')) as unique_owner_depts,
            
            -- Geography dimension
            COUNT(DISTINCT COALESCE(NULLIF(TRIM(community_area), ''), 'Unknown')) as unique_community_areas,
            COUNT(DISTINCT COALESCE(NULLIF(TRIM(ward), ''), 'Unknown')) as unique_wards,
            COUNT(DISTINCT COALESCE(NULLIF(TRIM(police_district), ''), 'Unknown')) as unique_police_districts,
            COUNT(DISTINCT COALESCE(NULLIF(TRIM(police_sector), ''), 'Unknown')) as unique_police_sectors,
            COUNT(DISTINCT COALESCE(NULLIF(TRIM(police_beat), ''), 'Unknown')) as unique_police_beats,
            
            -- Infrastructure dimension
            COUNT(DISTINCT COALESCE(NULLIF(TRIM(electrical_district), ''), 'Unknown')) as unique_electrical_districts,
            COUNT(DISTINCT COALESCE(NULLIF(TRIM(electricity_grid), ''), 'Unknown')) as unique_electricity_grids,
            
            -- Location dimension
            COUNT(DISTINCT COALESCE(NULLIF(TRIM(street_address), ''), 'Unknown')) as unique_addresses,
            COUNT(DISTINCT COALESCE(NULLIF(TRIM(zip_code), ''), 'Unknown')) as unique_zip_codes,
            COUNT(DISTINCT COALESCE(NULLIF(TRIM(city), ''), 'Unknown')) as unique_cities,
            
            -- Coordinates dimension
            COUNT(DISTINCT CASE 
                WHEN x_coordinate IS NOT NULL AND y_coordinate IS NOT NULL 
                THEN CONCAT(CAST(x_coordinate as VARCHAR), ',', CAST(y_coordinate as VARCHAR))
                ELSE 'Unknown' 
            END) as unique_coordinate_pairs,
            
            -- Time dimension (handle string dates)
            COUNT(DISTINCT TRY_CAST(LEFT(created_date, 10) AS DATE)) as unique_dates,
            COUNT(DISTINCT TRY_CAST(SUBSTRING(created_date, 12, 2) AS INTEGER)) as unique_hours
        FROM raw_requests_dedup_v2;
        """).fetchone()
        
        print(f"\n📈 Dimension Cardinality Analysis:")
        print(f"   🏢 Service Types: {dimension_analysis[0]:,}")
        print(f"   🏢 Service Short Codes: {dimension_analysis[1]:,}")  
        print(f"   🏢 Service Origins: {dimension_analysis[2]:,}")
        print(f"   🏛️  Departments (Created): {dimension_analysis[3]:,}")
        print(f"   🏛️  Departments (Owner): {dimension_analysis[4]:,}")
        print(f"   🌍 Community Areas: {dimension_analysis[5]:,}")
        print(f"   🌍 Wards: {dimension_analysis[6]:,}")
        print(f"   🚔 Police Districts: {dimension_analysis[7]:,}")
        print(f"   🚔 Police Sectors: {dimension_analysis[8]:,}")
        print(f"   🚔 Police Beats: {dimension_analysis[9]:,}")
        print(f"   ⚡ Electrical Districts: {dimension_analysis[10]:,}")
        print(f"   ⚡ Electricity Grids: {dimension_analysis[11]:,}")
        print(f"   📍 Unique Addresses: {dimension_analysis[12]:,}")
        print(f"   📮 ZIP Codes: {dimension_analysis[13]:,}")
        print(f"   🏙️  Cities: {dimension_analysis[14]:,}")
        print(f"   📊 Coordinate Pairs: {dimension_analysis[15]:,}")
        print(f"   📅 Unique Dates: {dimension_analysis[16]:,}")
        print(f"   🕐 Unique Hours: {dimension_analysis[17]:,}")
        
        # Date range analysis (handle string dates)
        print("\n⏳ Analyzing date ranges...")
        date_analysis = con.execute("""
        SELECT 
            MIN(TRY_CAST(created_date AS TIMESTAMP)) as earliest_date,
            MAX(TRY_CAST(created_date AS TIMESTAMP)) as latest_date,
            COUNT(DISTINCT TRY_CAST(LEFT(created_date, 4) AS INTEGER)) as unique_years,
            COUNT(DISTINCT TRY_CAST(LEFT(created_date, 7) AS VARCHAR)) as unique_months
        FROM raw_requests_dedup_v2 
        WHERE TRY_CAST(created_date AS TIMESTAMP) IS NOT NULL;
        """).fetchone()
        
        if date_analysis[0]:
            print(f"📅 Date Range: {date_analysis[0]} to {date_analysis[1]}")
            print(f"📅 Span: {date_analysis[2]} years, {date_analysis[3]} months")
        else:
            print("⚠️  No valid dates found in created_date column")
        
        print(f"\n✅ V2 setup complete! Your original tables are untouched.")
        print(f"🔄 Next: Run dimension creation scripts with _v2 suffix")

def compare_with_original():
    """
    Compare V2 results with original tables (if they exist)
    """
    with duckdb.connect(DB_PATH) as con:
        try:
            # Check if original tables exist
            original_count = con.execute("""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_name = 'fact_requests'
            """).fetchone()[0]
            
            v2_count = con.execute("""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_name = 'fact_requests_v2'
            """).fetchone()[0]
            
            if original_count > 0:
                orig_records = con.execute("SELECT COUNT(*) FROM fact_requests").fetchone()[0]
                print(f"📊 Original fact_requests: {orig_records:,} records")
            
            if v2_count > 0:
                v2_records = con.execute("SELECT COUNT(*) FROM fact_requests_v2").fetchone()[0] if v2_count > 0 else 0
                print(f"📊 V2 fact_requests: {v2_records:,} records")
                
        except Exception as e:
            print(f"ℹ️  Comparison not available: {e}")

def validate_source_columns():
    """
    Validate that all expected columns exist in raw.requests
    """
    expected_columns = [
        'sr_number', 'sr_type', 'sr_short_code', 'created_department', 
        'owner_department', 'status', 'origin', 'created_date', 
        'last_modified_date', 'closed_date', 'street_address', 'city', 
        'state', 'zip_code', 'street_number', 'street_direction', 
        'street_name', 'street_type', 'duplicate', 'legacy_record', 
        'legacy_sr_number', 'parent_sr_number', 'community_area', 'ward', 
        'electrical_district', 'electricity_grid', 'police_sector', 
        'police_district', 'police_beat', 'precinct', 'sanitation_division_days', 
        'created_hour', 'created_day_of_week', 'created_month', 
        'x_coordinate', 'y_coordinate', 'latitude', 'longitude', 'location'
    ]
    
    with duckdb.connect(DB_PATH) as con:
        try:
            # Get actual columns from the table
            actual_columns = con.execute("DESCRIBE raw.requests;").fetchall()
            actual_col_names = [row[0].lower() for row in actual_columns]
            
            missing_columns = [col for col in expected_columns if col.lower() not in actual_col_names]
            extra_columns = [col for col in actual_col_names if col not in [c.lower() for c in expected_columns]]
            
            print("\n🔍 Column Validation Results:")
            if missing_columns:
                print(f"❌ Missing expected columns: {missing_columns}")
                print("   These columns may need to be derived or have different names")
            if extra_columns:
                print(f"ℹ️  Extra columns found: {extra_columns[:10]}{'...' if len(extra_columns) > 10 else ''}")
            if not missing_columns:
                print("✅ All expected columns present")
            
            print(f"📋 Total columns in raw.requests: {len(actual_col_names)}")
                
        except Exception as e:
            print(f"❌ Could not validate columns: {e}")

if __name__ == "__main__":
    print("🚀 Starting Parallel ETL Process (V2)")
    print("=" * 50)
    validate_source_columns()
    print("=" * 50)
    setup_raw_requests_v2()
    print("=" * 50)
    compare_with_original()
    print("=" * 50)
    print("✅ Parallel ETL setup complete!")
    print("💡 Your original tables remain untouched")
    print("🔄 Next steps:")
    print("   1. Run dimension creation with _v2 suffix")
    print("   2. Build fact table with _v2 suffix") 
    print("   3. Compare results between original and V2")
    print("   4. Switch to V2 when satisfied")