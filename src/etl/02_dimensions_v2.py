"""
Enhanced dimension + fact creation script (v3)
Author: Data Engineering Team
Version: 3.0
Purpose: Create star-schema dimensions and fact table from Chicago 311 service requests
Notes:
  - Source: raw_requests_dedup_v2 (produced by 01_setup_v2.py)
  - Targets (all suffixed _v3 to avoid collisions):
      - fact_requests_staging_v3
      - dim_service_v3
      - dim_department_v3
      - dim_location_v3
      - dim_time_v3
      - dim_geography_v3
      - dim_infrastructure_v3
      - fact_requests_v3
Improvements:
  - Error handling, data profiling, performance metrics, structured logging
  - DuckDB-safe SQL (no LIKE ANY; no LAST_DAY; careful casting)
"""

import duckdb
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple
import json

# ---------------------------
# Logging
# ---------------------------
def setup_logging(log_level: str = "INFO") -> logging.Logger:
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    logger = logging.getLogger("etl.dimensions_v3")
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    logger.handlers.clear()

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)8s | %(funcName)s:%(lineno)d | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    fh = logging.FileHandler(log_dir / f"dimensions_v3_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

logger = setup_logging()

# ---------------------------
# Config loader (tomllib or toml)
# ---------------------------
def load_config() -> dict:
    cfg_path = Path("conf/config.toml")
    try:
        try:
            import tomllib  # Python 3.11+
            with cfg_path.open("rb") as f:
                cfg = tomllib.load(f)
                logger.info("✅ Config loaded via tomllib")
        except Exception:
            import toml  # fallback
            cfg = toml.load(str(cfg_path))
            logger.info("✅ Config loaded via toml package")
    except Exception as e:
        logger.warning(f"⚠️  Config not found or failed to parse ({e}); using defaults")
        cfg = {"db": {"path": "data/chicago_311.duckdb"}, "etl": {"debug": False}}
    # sanity
    if "db" not in cfg or "path" not in cfg["db"]:
        raise KeyError("Missing required config key: db.path")
    return cfg


# ---------------------------
# Builder
# ---------------------------
class StarBuilderV3:
    def __init__(self, config: dict):
        self.db_path = config["db"]["path"]
        self.debug = bool(config.get("etl", {}).get("debug", False))
        self.performance: Dict[str, float] = {}
        self.quality: Dict[str, dict] = {}
        self.source_table = "raw_requests_dedup_v2"  # aligned with 01_setup_v2.py
        self._validate_source()

    def _validate_source(self):
        with duckdb.connect(self.db_path) as con:
            cnt = con.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                [self.source_table],
            ).fetchone()[0]
            if cnt == 0:
                raise RuntimeError(f"Source table '{self.source_table}' not found. Run 01_setup_v2.py first.")
            n = con.execute(f"SELECT COUNT(*) FROM {self.source_table}").fetchone()[0]
            logger.info(f"✅ Source '{self.source_table}' is present with {n:,} rows")

    def _exec(self, con, sql: str, label: str, fetch: bool = False):
        t0 = datetime.now()
        try:
            cur = con.execute(sql)
            out = cur.fetchall() if fetch else None
            dt = (datetime.now() - t0).total_seconds()
            self.performance[label] = dt
            logger.debug(f"⏱️  {label}: {dt:.2f}s")
            return out
        except Exception as e:
            dt = (datetime.now() - t0).total_seconds()
            logger.error(f"❌ Failed: {label} after {dt:.2f}s | {e}")
            raise

    # ---------------------------
    # STAGING
    # ---------------------------
    def create_staging(self, con) -> None:
        logger.info("🧱 Creating staging table fact_requests_staging_v3 ...")
        self._exec(con, "DROP TABLE IF EXISTS fact_requests_staging_v3", "Drop staging")
        limit_clause = "LIMIT 10000" if self.debug else ""
        self._exec(
            con,
            f"""
            CREATE TABLE fact_requests_staging_v3 AS
            SELECT *
            FROM {self.source_table}
            {limit_clause}
            """,
            "Create staging",
        )
        n = con.execute("SELECT COUNT(*) FROM fact_requests_staging_v3").fetchone()[0]
        logger.info(f"   ✅ Staging rows: {n:,}")

    # ---------------------------
    # DIM: SERVICE
    # ---------------------------
    def create_dim_service(self, con) -> None:
        logger.info("🔧 Building dim_service_v3 ...")
        self._exec(con, "DROP TABLE IF EXISTS dim_service_v3", "Drop dim_service_v3")
        self._exec(
            con,
            """
            CREATE TABLE dim_service_v3 (
                service_id INTEGER PRIMARY KEY,
                service_name VARCHAR NOT NULL,
                service_short_code VARCHAR,
                service_origin VARCHAR,
                service_category VARCHAR NOT NULL,
                service_subcategory VARCHAR,
                priority_level VARCHAR DEFAULT 'Standard',
                is_emergency BOOLEAN DEFAULT FALSE,
                typical_resolution_days INTEGER DEFAULT 7,
                service_hash VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            "Create dim_service_v3",
        )

        # DuckDB: expand LIKE conditions with ORs
        self._exec(
            con,
            f"""
            INSERT INTO dim_service_v3(
                service_id, service_name, service_short_code, service_origin,
                service_category, service_subcategory, priority_level,
                is_emergency, typical_resolution_days, service_hash
            )
            SELECT
                ROW_NUMBER() OVER (ORDER BY COALESCE(sr_type,''), COALESCE(sr_short_code,''), COALESCE(origin,'')) AS service_id,
                COALESCE(NULLIF(TRIM(sr_type), ''), 'Unknown Service') AS service_name,
                COALESCE(NULLIF(TRIM(sr_short_code), ''), 'UNK') AS service_short_code,
                COALESCE(NULLIF(TRIM(origin), ''), 'Unknown Origin') AS service_origin,

                CASE
                    WHEN LOWER(COALESCE(sr_type,'')) LIKE '%graffiti%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%litter%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%garbage%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%refuse%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%waste%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%debris%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%dumping%' THEN 'Sanitation'
                    WHEN LOWER(COALESCE(sr_type,'')) LIKE '%pothole%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%street%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%traffic%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%sidewalk%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%curb%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%pavement%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%road%' THEN 'Transportation'
                    WHEN LOWER(COALESCE(sr_type,'')) LIKE '%tree%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%vegetation%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%park%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%landscape%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%green%' THEN 'Environment'
                    WHEN LOWER(COALESCE(sr_type,'')) LIKE '%light%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%electrical%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%power%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%utility%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%signal%' THEN 'Infrastructure'
                    WHEN LOWER(COALESCE(sr_type,'')) LIKE '%water%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%sewer%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%drain%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%flood%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%leak%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%pipe%' THEN 'Utilities'
                    WHEN LOWER(COALESCE(sr_type,'')) LIKE '%animal%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%rodent%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%pest%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%stray%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%wildlife%' THEN 'Animal Services'
                    WHEN LOWER(COALESCE(sr_type,'')) LIKE '%abandon%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%building%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%property%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%vacant%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%structure%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%housing%' THEN 'Property'
                    WHEN LOWER(COALESCE(sr_type,'')) LIKE '%noise%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%police%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%crime%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%safety%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%security%' THEN 'Public Safety'
                    WHEN LOWER(COALESCE(sr_type,'')) LIKE '%permit%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%license%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%violation%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%inspection%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%code%' THEN 'Regulatory'
                    WHEN LOWER(COALESCE(sr_type,'')) LIKE '%health%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%medical%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%food%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%restaurant%' THEN 'Health Services'
                    ELSE 'Other'
                END AS service_category,

                CASE
                    WHEN LOWER(COALESCE(sr_type,'')) LIKE '%graffiti%' THEN 'Graffiti Removal'
                    WHEN LOWER(COALESCE(sr_type,'')) LIKE '%pothole%' THEN 'Road Maintenance'
                    WHEN LOWER(COALESCE(sr_type,'')) LIKE '%tree%' THEN 'Tree Services'
                    WHEN LOWER(COALESCE(sr_type,'')) LIKE '%light%' THEN 'Street Lighting'
                    WHEN LOWER(COALESCE(sr_type,'')) LIKE '%water%' THEN 'Water Services'
                    ELSE 'General'
                END AS service_subcategory,

                CASE
                    WHEN LOWER(COALESCE(sr_type,'')) LIKE '%emergency%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%urgent%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%danger%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%hazard%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%leak%' THEN 'High'
                    WHEN LOWER(COALESCE(sr_type,'')) LIKE '%safety%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%traffic%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%signal%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%water%' THEN 'Medium'
                    ELSE 'Standard'
                END AS priority_level,

                CASE
                    WHEN LOWER(COALESCE(sr_type,'')) LIKE '%emergency%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%urgent%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%danger%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%hazard%' THEN TRUE
                    ELSE FALSE
                END AS is_emergency,

                CASE
                    WHEN LOWER(COALESCE(sr_type,'')) LIKE '%emergency%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%urgent%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%danger%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%hazard%' THEN 1
                    WHEN LOWER(COALESCE(sr_type,'')) LIKE '%graffiti%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%litter%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%light%' THEN 3
                    WHEN LOWER(COALESCE(sr_type,'')) LIKE '%pothole%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%tree%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%water%' THEN 7
                    WHEN LOWER(COALESCE(sr_type,'')) LIKE '%building%'
                      OR LOWER(COALESCE(sr_type,'')) LIKE '%property%' THEN 14
                    ELSE 7
                END AS typical_resolution_days,

                md5(CONCAT(
                    COALESCE(TRIM(sr_type),''),'|',
                    COALESCE(TRIM(sr_short_code),''),'|',
                    COALESCE(TRIM(origin),'')
                )) AS service_hash
            FROM (
                SELECT DISTINCT sr_type, sr_short_code, origin
                FROM {self.source_table}
                WHERE sr_type IS NOT NULL OR sr_short_code IS NOT NULL OR origin IS NOT NULL
            ) s
            """,
            "Insert dim_service_v3",
        )
        self._profile_dim(con, "dim_service_v3", ["service_name", "service_category"])

    # ---------------------------
    # DIM: DEPARTMENT (agency)
    # ---------------------------
    def create_dim_department(self, con) -> None:
        logger.info("🏛️  Building dim_department_v3 ...")
        self._exec(con, "DROP TABLE IF EXISTS dim_department_v3", "Drop dim_department_v3")
        self._exec(
            con,
            """
            CREATE TABLE dim_department_v3 (
                department_id INTEGER PRIMARY KEY,
                department_name VARCHAR NOT NULL,
                created_department VARCHAR,
                department_type VARCHAR NOT NULL,
                department_hierarchy VARCHAR,
                is_missing_data BOOLEAN DEFAULT FALSE,
                agency_capacity VARCHAR DEFAULT 'Medium',
                operating_hours VARCHAR DEFAULT 'Business Hours',
                department_hash VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            "Create dim_department_v3",
        )
        self._exec(
            con,
            f"""
            INSERT INTO dim_department_v3(
                department_id, department_name, created_department, department_type,
                department_hierarchy, is_missing_data, agency_capacity, operating_hours, department_hash
            )
            SELECT
                ROW_NUMBER() OVER (
                    ORDER BY COALESCE(NULLIF(TRIM(owner_department),''),'Unknown Department'),
                             COALESCE(NULLIF(TRIM(created_department),''),'Unknown Creator')
                ) AS department_id,
                COALESCE(NULLIF(TRIM(owner_department),''), 'Unknown Department') AS department_name,
                COALESCE(NULLIF(TRIM(created_department),''), 'Unknown Creator') AS created_department,

                CASE
                    WHEN LOWER(COALESCE(owner_department,'')) LIKE '%water%'
                      OR LOWER(COALESCE(owner_department,'')) LIKE '%sewer%'
                      OR LOWER(COALESCE(owner_department,'')) LIKE '%utility%' THEN 'Utilities'
                    WHEN LOWER(COALESCE(owner_department,'')) LIKE '%transport%'
                      OR LOWER(COALESCE(owner_department,'')) LIKE '%street%'
                      OR LOWER(COALESCE(owner_department,'')) LIKE '%traffic%'
                      OR LOWER(COALESCE(owner_department,'')) LIKE '%cdot%' THEN 'Transportation'
                    WHEN LOWER(COALESCE(owner_department,'')) LIKE '%sanitation%'
                      OR LOWER(COALESCE(owner_department,'')) LIKE '%environment%'
                      OR LOWER(COALESCE(owner_department,'')) LIKE '%fleet%'
                      OR LOWER(COALESCE(owner_department,'')) LIKE '%waste%' THEN 'Environmental'
                    WHEN LOWER(COALESCE(owner_department,'')) LIKE '%police%'
                      OR LOWER(COALESCE(owner_department,'')) LIKE '%fire%'
                      OR LOWER(COALESCE(owner_department,'')) LIKE '%emergency%'
                      OR LOWER(COALESCE(owner_department,'')) LIKE '%oemc%' THEN 'Public Safety'
                    WHEN LOWER(COALESCE(owner_department,'')) LIKE '%building%'
                      OR LOWER(COALESCE(owner_department,'')) LIKE '%housing%'
                      OR LOWER(COALESCE(owner_department,'')) LIKE '%planning%'
                      OR LOWER(COALESCE(owner_department,'')) LIKE '%zoning%' THEN 'Development'
                    WHEN LOWER(COALESCE(owner_department,'')) LIKE '%health%'
                      OR LOWER(COALESCE(owner_department,'')) LIKE '%family%'
                      OR LOWER(COALESCE(owner_department,'')) LIKE '%social%'
                      OR LOWER(COALESCE(owner_department,'')) LIKE '%human%' THEN 'Health & Human Services'
                    WHEN LOWER(COALESCE(owner_department,'')) LIKE '%park%'
                      OR LOWER(COALESCE(owner_department,'')) LIKE '%recreation%'
                      OR LOWER(COALESCE(owner_department,'')) LIKE '%cultural%' THEN 'Parks & Recreation'
                    WHEN COALESCE(NULLIF(TRIM(owner_department),''), 'Unknown Department') = 'Unknown Department' THEN 'Missing Data'
                    ELSE 'Administrative'
                END AS department_type,

                CASE
                    WHEN LOWER(COALESCE(owner_department,'')) LIKE '%commissioner%'
                      OR LOWER(COALESCE(owner_department,'')) LIKE '%director%'
                      OR LOWER(COALESCE(owner_department,'')) LIKE '%chief%' THEN 'Executive'
                    WHEN LOWER(COALESCE(owner_department,'')) LIKE '%manager%'
                      OR LOWER(COALESCE(owner_department,'')) LIKE '%supervisor%'
                      OR LOWER(COALESCE(owner_department,'')) LIKE '%coordinator%' THEN 'Management'
                    WHEN LOWER(COALESCE(owner_department,'')) LIKE '%officer%'
                      OR LOWER(COALESCE(owner_department,'')) LIKE '%inspector%'
                      OR LOWER(COALESCE(owner_department,'')) LIKE '%specialist%' THEN 'Operational'
                    ELSE 'Standard'
                END AS department_hierarchy,

                CASE WHEN NULLIF(TRIM(owner_department),'') IS NULL THEN TRUE ELSE FALSE END AS is_missing_data,

                CASE
                    WHEN LOWER(COALESCE(owner_department,'')) LIKE '%police%'
                      OR LOWER(COALESCE(owner_department,'')) LIKE '%fire%'
                      OR LOWER(COALESCE(owner_department,'')) LIKE '%water%'
                      OR LOWER(COALESCE(owner_department,'')) LIKE '%transport%' THEN 'High'
                    WHEN LOWER(COALESCE(owner_department,'')) LIKE '%health%'
                      OR LOWER(COALESCE(owner_department,'')) LIKE '%building%'
                      OR LOWER(COALESCE(owner_department,'')) LIKE '%environment%' THEN 'Medium'
                    ELSE 'Standard'
                END AS agency_capacity,

                CASE
                    WHEN LOWER(COALESCE(owner_department,'')) LIKE '%police%'
                      OR LOWER(COALESCE(owner_department,'')) LIKE '%fire%'
                      OR LOWER(COALESCE(owner_department,'')) LIKE '%emergency%'
                      OR LOWER(COALESCE(owner_department,'')) LIKE '%water%' THEN '24/7'
                    WHEN LOWER(COALESCE(owner_department,'')) LIKE '%sanitation%'
                      OR LOWER(COALESCE(owner_department,'')) LIKE '%transport%'
                      OR LOWER(COALESCE(owner_department,'')) LIKE '%street%' THEN 'Extended Hours'
                    ELSE 'Business Hours'
                END AS operating_hours,

                md5(CONCAT(
                    COALESCE(TRIM(owner_department),''),'|',
                    COALESCE(TRIM(created_department),'')
                )) AS department_hash
            FROM (
                SELECT DISTINCT owner_department, created_department
                FROM {self.source_table}
            ) d
            """,
            "Insert dim_department_v3",
        )
        self._profile_dim(con, "dim_department_v3", ["department_name", "department_type"])

    # # ---------------------------
    # # DIM: LOCATION
    # # ---------------------------
    # def create_dim_location(self, con) -> None:
    #     logger.info("📍 Building dim_location_v3 ...")
    #     self._exec(con, "DROP TABLE IF EXISTS dim_location_v3", "Drop dim_location_v3")
    #     self._exec(
    #         con,
    #         """
    #         CREATE TABLE dim_location_v3 (
    #             location_id INTEGER PRIMARY KEY,
    #             street_number VARCHAR,
    #             street_name VARCHAR,
    #             street_type VARCHAR,
    #             street_direction VARCHAR,
    #             street_address VARCHAR,
    #             city VARCHAR DEFAULT 'Chicago',
    #             state VARCHAR DEFAULT 'IL',
    #             zip_code VARCHAR,
    #             community_area VARCHAR,
    #             ward INTEGER,
    #             police_district VARCHAR,
    #             police_beat VARCHAR,
    #             police_sector VARCHAR,
    #             precinct VARCHAR,
    #             sanitation_division_days VARCHAR,
    #             electrical_district VARCHAR,
    #             electricity_grid VARCHAR,
    #             latitude DOUBLE,
    #             longitude DOUBLE,
    #             x_coordinate DOUBLE,
    #             y_coordinate DOUBLE,
    #             location_key VARCHAR UNIQUE,
    #             address_completeness VARCHAR NOT NULL,
    #             geographic_region VARCHAR,
    #             spatial_quality_score DOUBLE DEFAULT 0.0,
    #             is_geocoded BOOLEAN DEFAULT FALSE,
    #             coordinate_precision VARCHAR DEFAULT 'Unknown',
    #             created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    #             updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    #         )
    #         """,
    #         "Create dim_location_v3",
    #     )
    #     self._exec(
    #         con,
    #         f"""
    #         INSERT INTO dim_location_v3(
    #             location_id, street_number, street_name, street_type, street_direction, street_address,
    #             city, state, zip_code, community_area, ward, police_district, police_beat,
    #             police_sector, precinct, sanitation_division_days, electrical_district, electricity_grid,
    #             latitude, longitude, x_coordinate, y_coordinate, location_key,
    #             address_completeness, geographic_region, spatial_quality_score,
    #             is_geocoded, coordinate_precision
    #         )
    #         SELECT
    #             ROW_NUMBER() OVER (ORDER BY location_key) AS location_id,
    #             NULLIF(TRIM(street_number), '') AS street_number,
    #             NULLIF(TRIM(street_name), '') AS street_name,
    #             NULLIF(TRIM(street_type), '') AS street_type,
    #             NULLIF(TRIM(street_direction), '') AS street_direction,
    #             NULLIF(TRIM(street_address), '') AS street_address,
    #             COALESCE(NULLIF(TRIM(city), ''), 'Chicago') AS city,
    #             COALESCE(NULLIF(TRIM(state), ''), 'IL') AS state,
    #             NULLIF(TRIM(zip_code), '') AS zip_code,
    #             NULLIF(TRIM(community_area), '') AS community_area,
    #             TRY_CAST(NULLIF(TRIM(ward), '') AS INTEGER) AS ward,
    #             NULLIF(TRIM(police_district), '') AS police_district,
    #             NULLIF(TRIM(police_beat), '') AS police_beat,
    #             NULLIF(TRIM(police_sector), '') AS police_sector,
    #             NULLIF(TRIM(precinct), '') AS precinct,
    #             NULLIF(TRIM(sanitation_division_days), '') AS sanitation_division_days,
    #             NULLIF(TRIM(electrical_district), '') AS electrical_district,
    #             NULLIF(TRIM(electricity_grid), '') AS electricity_grid,
    #             TRY_CAST(latitude AS DOUBLE) AS latitude,
    #             TRY_CAST(longitude AS DOUBLE) AS longitude,
    #             TRY_CAST(x_coordinate AS DOUBLE) AS x_coordinate,
    #             TRY_CAST(y_coordinate AS DOUBLE) AS y_coordinate,
    #             location_key,

    #             CASE
    #                 WHEN street_address IS NOT NULL AND zip_code IS NOT NULL AND latitude IS NOT NULL AND longitude IS NOT NULL THEN 'Complete'
    #                 WHEN street_address IS NOT NULL AND (zip_code IS NOT NULL OR (latitude IS NOT NULL AND longitude IS NOT NULL)) THEN 'Good'
    #                 WHEN street_name IS NOT NULL OR latitude IS NOT NULL THEN 'Partial'
    #                 ELSE 'Poor'
    #             END AS address_completeness,

    #             CASE
    #                 WHEN TRY_CAST(ward AS INTEGER) BETWEEN 1 AND 10 THEN 'North Chicago'
    #                 WHEN TRY_CAST(ward AS INTEGER) BETWEEN 11 AND 20 THEN 'Northwest Chicago'
    #                 WHEN TRY_CAST(ward AS INTEGER) BETWEEN 21 AND 30 THEN 'Central Chicago'
    #                 WHEN TRY_CAST(ward AS INTEGER) BETWEEN 31 AND 40 THEN 'Southwest Chicago'
    #                 WHEN TRY_CAST(ward AS INTEGER) BETWEEN 41 AND 50 THEN 'South Chicago'
    #                 WHEN community_area IS NOT NULL THEN 'Chicago (Community Based)'
    #                 WHEN TRY_CAST(latitude AS DOUBLE) BETWEEN 41.6 AND 42.1 AND TRY_CAST(longitude AS DOUBLE) BETWEEN -87.9 AND -87.5 THEN 'Chicago (Coordinate Based)'
    #                 ELSE 'Unknown Region'
    #             END AS geographic_region,

    #             CASE 
    #             WHEN TRY_CAST(latitude AS DOUBLE) IS NOT NULL 
    #             AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL 
    #             AND street_address IS NOT NULL 
    #             AND ward IS NOT NULL THEN 1.0
    #             WHEN TRY_CAST(latitude AS DOUBLE) IS NOT NULL 
    #             AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL 
    #             AND street_address IS NOT NULL THEN 0.8
    #             WHEN TRY_CAST(latitude AS DOUBLE) IS NOT NULL 
    #             AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL THEN 0.6
    #             WHEN street_address IS NOT NULL AND ward IS NOT NULL THEN 0.4
    #             WHEN street_address IS NOT NULL THEN 0.2
    #             ELSE 0.0
    #             END AS spatial_quality_score,


    #             CASE WHEN TRY_CAST(latitude AS DOUBLE) IS NOT NULL AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL THEN TRUE ELSE FALSE 
    #             END AS is_geocoded,
                
    #             CASE
    #                WHEN TRY_CAST(latitude AS DOUBLE) IS NOT NULL 
    #                AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL
    #                AND street_address IS NOT NULL THEN 'Address Level'
    #                WHEN TRY_CAST(latitude AS DOUBLE) IS NOT NULL
    #                AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL
    #                AND street_name IS NOT NULL THEN 'Street Level'
    #                WHEN TRY_CAST(latitude AS DOUBLE) IS NOT NULL
    #                AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL THEN 'Area Level'
    #                ELSE 'No Coordinates'
    #         END AS coordinate_precision

    #        FROM (
    #             SELECT
    #                 street_number, street_name, street_type, street_direction, street_address,
    #                 city, state, zip_code, community_area, ward, police_district, police_beat,
    #                 police_sector, precinct, sanitation_division_days, electrical_district, electricity_grid,
    #                 latitude, longitude, x_coordinate, y_coordinate,
    #                 md5(CONCAT(
    #                     COALESCE(TRIM(street_address),'NO_ADDRESS'),'|',
    #                     COALESCE(TRIM(city),'NO_CITY'),'|',
    #                     COALESCE(TRIM(zip_code),'NO_ZIP'),'|',
    #                     COALESCE(CAST(ROUND(TRY_CAST(latitude AS DOUBLE),6) AS VARCHAR),'NO_LAT'),'|',
    #                     COALESCE(CAST(ROUND(TRY_CAST(longitude AS DOUBLE),6) AS VARCHAR),'NO_LON'),'|',
    #                     COALESCE(TRIM(ward),'NO_WARD')
    #                 )) AS location_key
    #             FROM {self.source_table}
    #             GROUP BY location_key
    #         ) t
    #         """,
    #         "Insert dim_location_v3",
    #     )
    #     self._profile_dim(con, "dim_location_v3", ["street_address", "ward"])
    
    

    # ---------------------------
    # DIM: TIME
    # ---------------------------
    def create_dim_time(self, con) -> None:
        logger.info("📅 Building dim_time_v3 ...")
        
        # Drop existing table
        self._exec(con, "DROP TABLE IF EXISTS dim_time_v3", "Drop dim_time_v3")
        
        # Create table
        self._exec(
            con,
            """
            CREATE TABLE dim_time_v3 (
                date_id DATE PRIMARY KEY,
                year INT NOT NULL,
                month INT NOT NULL,
                day INT NOT NULL,
                quarter INT NOT NULL,
                day_of_week INT NOT NULL,
                day_name VARCHAR NOT NULL,
                month_name VARCHAR NOT NULL,
                is_weekend BOOLEAN DEFAULT FALSE,
                is_holiday BOOLEAN DEFAULT FALSE,
                is_business_day BOOLEAN DEFAULT TRUE,
                season VARCHAR NOT NULL,
                service_season VARCHAR NOT NULL,
                fiscal_year INT,
                fiscal_quarter INT,
                week_of_year INT,
                week_of_month INT,
                day_of_year INT,
                is_month_start BOOLEAN DEFAULT FALSE,
                is_month_end BOOLEAN DEFAULT FALSE,
                is_quarter_start BOOLEAN DEFAULT FALSE,
                is_quarter_end BOOLEAN DEFAULT FALSE,
                is_year_start BOOLEAN DEFAULT FALSE,
                is_year_end BOOLEAN DEFAULT FALSE,
                holiday_name VARCHAR,
                business_days_in_month INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            "Create dim_time_v3",
        )
        
        # Insert enriched time data with created_at
        self._exec(
            con,
            f"""
            WITH dates AS (
                SELECT DISTINCT CAST(TRY_CAST(created_date AS TIMESTAMP) AS DATE) AS d
                FROM {self.source_table}
                WHERE TRY_CAST(created_date AS TIMESTAMP) IS NOT NULL
            ),
            enriched AS (
                SELECT
                    d AS date_id,
                    EXTRACT(YEAR FROM d) AS year,
                    EXTRACT(MONTH FROM d) AS month,
                    EXTRACT(DAY FROM d) AS day,
                    EXTRACT(QUARTER FROM d) AS quarter,
                    EXTRACT(DOW FROM d) AS day_of_week,
                    STRFTIME(d, '%A') AS day_name,
                    STRFTIME(d, '%B') AS month_name,
                    CASE WHEN EXTRACT(DOW FROM d) IN (0,6) THEN TRUE ELSE FALSE END AS is_weekend,
                    CASE
                        WHEN STRFTIME(d, '%m-%d') IN ('01-01','07-04','12-25','11-11','02-14','10-31') THEN TRUE
                        WHEN EXTRACT(MONTH FROM d)=1 AND EXTRACT(DOW FROM d)=1 AND EXTRACT(DAY FROM d) BETWEEN 15 AND 21 THEN TRUE
                        WHEN EXTRACT(MONTH FROM d)=2 AND EXTRACT(DOW FROM d)=1 AND EXTRACT(DAY FROM d) BETWEEN 15 AND 21 THEN TRUE
                        WHEN EXTRACT(MONTH FROM d)=5 AND EXTRACT(DOW FROM d)=1 AND EXTRACT(DAY FROM d) > 24 THEN TRUE
                        WHEN EXTRACT(MONTH FROM d)=9 AND EXTRACT(DOW FROM d)=1 AND EXTRACT(DAY FROM d) <= 7 THEN TRUE
                        WHEN EXTRACT(MONTH FROM d)=11 AND EXTRACT(DOW FROM d)=4 AND EXTRACT(DAY FROM d) BETWEEN 22 AND 28 THEN TRUE
                        ELSE FALSE
                    END AS is_holiday,
                    CASE
                        WHEN EXTRACT(DOW FROM d) IN (0,6) THEN FALSE
                        WHEN STRFTIME(d, '%m-%d') IN ('01-01','07-04','12-25','11-11') THEN FALSE
                        WHEN EXTRACT(MONTH FROM d)=11 AND EXTRACT(DOW FROM d)=4 AND EXTRACT(DAY FROM d) BETWEEN 22 AND 28 THEN FALSE
                        ELSE TRUE
                    END AS is_business_day,
                    CASE
                        WHEN EXTRACT(MONTH FROM d) IN (6,7,8) THEN 'Summer'
                        WHEN EXTRACT(MONTH FROM d) IN (12,1,2) THEN 'Winter'
                        WHEN EXTRACT(MONTH FROM d) IN (3,4,5) THEN 'Spring'
                        ELSE 'Fall'
                    END AS season,
                    CASE
                        WHEN EXTRACT(MONTH FROM d) IN (11,12,1,2,3) THEN 'Winter Service Peak'
                        WHEN EXTRACT(MONTH FROM d) IN (6,7,8) THEN 'Summer Service Peak'
                        ELSE 'Regular Season'
                    END AS service_season,
                    CASE WHEN EXTRACT(MONTH FROM d) >= 7 THEN EXTRACT(YEAR FROM d) + 1 ELSE EXTRACT(YEAR FROM d) END AS fiscal_year,
                    CASE
                        WHEN EXTRACT(MONTH FROM d) IN (7,8,9) THEN 1
                        WHEN EXTRACT(MONTH FROM d) IN (10,11,12) THEN 2
                        WHEN EXTRACT(MONTH FROM d) IN (1,2,3) THEN 3
                        ELSE 4
                    END AS fiscal_quarter,
                    EXTRACT(WEEK FROM d) AS week_of_year,
                    CEIL(EXTRACT(DAY FROM d) / 7.0) AS week_of_month,
                    EXTRACT(DOY FROM d) AS day_of_year,
                    CASE WHEN EXTRACT(DAY FROM d) = 1 THEN TRUE ELSE FALSE END AS is_month_start,
                    CASE WHEN d = (DATE_TRUNC('month', d) + INTERVAL 1 MONTH - INTERVAL 1 DAY) THEN TRUE ELSE FALSE END AS is_month_end,
                    CASE WHEN EXTRACT(DAY FROM d) = 1 AND EXTRACT(MONTH FROM d) IN (1,4,7,10) THEN TRUE ELSE FALSE END AS is_quarter_start,
                    CASE WHEN d = (DATE_TRUNC('month', d) + INTERVAL 1 MONTH - INTERVAL 1 DAY)
                            AND EXTRACT(MONTH FROM d) IN (3,6,9,12) THEN TRUE ELSE FALSE END AS is_quarter_end,
                    CASE WHEN STRFTIME(d, '%m-%d') = '01-01' THEN TRUE ELSE FALSE END AS is_year_start,
                    CASE WHEN STRFTIME(d, '%m-%d') = '12-31' THEN TRUE ELSE FALSE END AS is_year_end,
                    CASE
                        WHEN STRFTIME(d, '%m-%d') = '01-01' THEN 'New Years Day'
                        WHEN STRFTIME(d, '%m-%d') = '07-04' THEN 'Independence Day'
                        WHEN STRFTIME(d, '%m-%d') = '12-25' THEN 'Christmas Day'
                        WHEN STRFTIME(d, '%m-%d') = '11-11' THEN 'Veterans Day'
                        WHEN EXTRACT(MONTH FROM d)=1 AND EXTRACT(DOW FROM d)=1 AND EXTRACT(DAY FROM d) BETWEEN 15 AND 21 THEN 'Martin Luther King Jr Day'
                        WHEN EXTRACT(MONTH FROM d)=2 AND EXTRACT(DOW FROM d)=1 AND EXTRACT(DAY FROM d) BETWEEN 15 AND 21 THEN 'Presidents Day'
                        WHEN EXTRACT(MONTH FROM d)=5 AND EXTRACT(DOW FROM d)=1 AND EXTRACT(DAY FROM d) > 24 THEN 'Memorial Day'
                        WHEN EXTRACT(MONTH FROM d)=9 AND EXTRACT(DOW FROM d)=1 AND EXTRACT(DAY FROM d) <= 7 THEN 'Labor Day'
                        WHEN EXTRACT(MONTH FROM d)=11 AND EXTRACT(DOW FROM d)=4 AND EXTRACT(DAY FROM d) BETWEEN 22 AND 28 THEN 'Thanksgiving'
                        ELSE NULL
                    END AS holiday_name,
                    CASE
                        WHEN EXTRACT(MONTH FROM d) = 2 THEN 20
                        WHEN EXTRACT(MONTH FROM d) IN (1,3,5,7,8,10,12) THEN 23
                        ELSE 22
                    END AS business_days_in_month,
                    CURRENT_TIMESTAMP AS created_at
                FROM dates
            )
            INSERT INTO dim_time_v3
            SELECT * FROM enriched
            ORDER BY date_id
            """,
            "Insert dim_time_v3",
        )
        
        # Profile the dimension
        self._profile_dim(con, "dim_time_v3", ["year", "month", "season"])


    # ---------------------------
    # DIM: GEOGRAPHY
    # ---------------------------
    def create_dim_geography(self, con) -> None:
        logger.info("🗺️  Building dim_geography_v3 ...")
        self._exec(con, "DROP TABLE IF EXISTS dim_geography_v3", "Drop dim_geography_v3")
        self._exec(
            con,
            """
            CREATE TABLE dim_geography_v3 (
                geography_id INTEGER PRIMARY KEY,
                community_area VARCHAR,
                community_area_name VARCHAR,
                ward INT,
                ward_name VARCHAR,
                police_district VARCHAR,
                police_beat VARCHAR,
                police_sector VARCHAR,
                precinct VARCHAR,
                alderman_name VARCHAR,
                geographic_cluster VARCHAR,
                service_area VARCHAR,
                population_density VARCHAR DEFAULT 'Unknown',
                socioeconomic_level VARCHAR DEFAULT 'Mixed',
                geography_hash VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            "Create dim_geography_v3",
        )
        self._exec(
            con,
            f"""
            INSERT INTO dim_geography_v3(
                geography_id, community_area, community_area_name, ward, ward_name,
                police_district, police_beat, police_sector, precinct, alderman_name,
                geographic_cluster, service_area, population_density, socioeconomic_level, geography_hash
            )
            SELECT
                ROW_NUMBER() OVER (
                    ORDER BY COALESCE(community_area,'Unknown'),
                             COALESCE(CAST(ward AS VARCHAR),'Unknown'),
                             COALESCE(police_district,'Unknown')
                ) AS geography_id,

                NULLIF(TRIM(community_area),'') AS community_area,

                CASE
                    WHEN NULLIF(TRIM(community_area),'') IS NOT NULL
                        THEN CONCAT('Community Area ', TRIM(community_area))
                    ELSE NULL
                END AS community_area_name,

                TRY_CAST(NULLIF(TRIM(ward),'') AS INTEGER) AS ward,

                CASE
                    WHEN TRY_CAST(ward AS INTEGER) IS NOT NULL
                        THEN CONCAT('Ward ', CAST(TRY_CAST(ward AS INTEGER) AS VARCHAR))
                    ELSE NULL
                END AS ward_name,

                NULLIF(TRIM(police_district),'') AS police_district,
                NULLIF(TRIM(police_beat),'') AS police_beat,
                NULLIF(TRIM(police_sector),'') AS police_sector,
                NULLIF(TRIM(precinct),'') AS precinct,

                CASE
                    WHEN TRY_CAST(ward AS INTEGER) IS NOT NULL THEN CONCAT('Alderman Ward ', CAST(TRY_CAST(ward AS INTEGER) AS VARCHAR))
                    ELSE NULL
                END AS alderman_name,

                CASE
                    WHEN TRY_CAST(ward AS INTEGER) BETWEEN 1 AND 17 THEN 'North Side Cluster'
                    WHEN TRY_CAST(ward AS INTEGER) BETWEEN 18 AND 34 THEN 'Central Cluster'
                    WHEN TRY_CAST(ward AS INTEGER) BETWEEN 35 AND 50 THEN 'South Side Cluster'
                    WHEN community_area IS NOT NULL THEN 'Community Based Cluster'
                    ELSE 'Unassigned Cluster'
                END AS geographic_cluster,

                CASE
                    WHEN police_district IS NOT NULL THEN CONCAT('Police District ', police_district)
                    WHEN TRY_CAST(ward AS INTEGER) IS NOT NULL THEN CONCAT('Ward Service Area ', CAST(TRY_CAST(ward AS INTEGER) AS VARCHAR))
                    ELSE 'Unassigned Service Area'
                END AS service_area,

                CASE
                    WHEN TRY_CAST(ward AS INTEGER) IN (1,2,8,32,42,43) THEN 'High Density'
                    WHEN TRY_CAST(ward AS INTEGER) IN (19,23,38,44,46,47,48,49,50) THEN 'Low Density'
                    ELSE 'Medium Density'
                END AS population_density,

                CASE
                    WHEN TRY_CAST(ward AS INTEGER) IN (1,2,18,19,38,39,40,41,43) THEN 'Higher Income'
                    WHEN TRY_CAST(ward AS INTEGER) IN (15,16,17,20,24,28,29,34) THEN 'Lower Income'
                    ELSE 'Mixed Income'
                END AS socioeconomic_level,

                md5(CONCAT(
                    COALESCE(TRIM(community_area),''),'|',
                    COALESCE(TRIM(ward),''),'|',
                    COALESCE(TRIM(police_district),''),'|',
                    COALESCE(TRIM(police_beat),''),'|',
                    COALESCE(TRIM(police_sector),''),'|',
                    COALESCE(TRIM(precinct),'')
                )) AS geography_hash
            FROM (
                SELECT DISTINCT community_area, ward, police_district, police_beat, police_sector, precinct
                FROM {self.source_table}
                WHERE community_area IS NOT NULL OR ward IS NOT NULL OR police_district IS NOT NULL
            ) g
            """,
            "Insert dim_geography_v3",
        )
        self._profile_dim(con, "dim_geography_v3", ["ward", "police_district"])

    # ---------------------------
    # DIM: INFRASTRUCTURE
    # ---------------------------
    def create_dim_infrastructure(self, con) -> None:
        logger.info("⚡ Building dim_infrastructure_v3 ...")
        self._exec(con, "DROP TABLE IF EXISTS dim_infrastructure_v3", "Drop dim_infrastructure_v3")
        self._exec(
            con,
            """
            CREATE TABLE dim_infrastructure_v3 (
                infrastructure_id INTEGER PRIMARY KEY,
                electrical_district VARCHAR,
                electricity_grid VARCHAR,
                sanitation_division_days VARCHAR,
                utility_type VARCHAR,
                service_reliability VARCHAR DEFAULT 'Standard',
                maintenance_schedule VARCHAR,
                infrastructure_age VARCHAR DEFAULT 'Unknown',
                capacity_level VARCHAR DEFAULT 'Normal',
                infrastructure_hash VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            "Create dim_infrastructure_v3",
        )
        self._exec(
            con,
            f"""
            INSERT INTO dim_infrastructure_v3(
                infrastructure_id, electrical_district, electricity_grid, sanitation_division_days,
                utility_type, service_reliability, maintenance_schedule, infrastructure_age,
                capacity_level, infrastructure_hash
            )
            SELECT
                ROW_NUMBER() OVER (
                    ORDER BY COALESCE(electrical_district,'Unknown'),
                             COALESCE(electricity_grid,'Unknown'),
                             COALESCE(sanitation_division_days,'Unknown')
                ) AS infrastructure_id,

                NULLIF(TRIM(electrical_district),'') AS electrical_district,
                NULLIF(TRIM(electricity_grid),'') AS electricity_grid,
                NULLIF(TRIM(sanitation_division_days),'') AS sanitation_division_days,

                CASE
                    WHEN electrical_district IS NOT NULL AND electricity_grid IS NOT NULL THEN 'Electrical'
                    WHEN sanitation_division_days IS NOT NULL THEN 'Sanitation'
                    WHEN electrical_district IS NOT NULL THEN 'Power Distribution'
                    ELSE 'General Utility'
                END AS utility_type,

                CASE
                    WHEN electrical_district IS NOT NULL AND electricity_grid IS NOT NULL THEN 'High Reliability'
                    WHEN sanitation_division_days LIKE '%DAILY%' OR sanitation_division_days LIKE '%MON%' THEN 'High Frequency'
                    WHEN electrical_district IS NOT NULL OR electricity_grid IS NOT NULL THEN 'Standard Reliability'
                    ELSE 'Variable'
                END AS service_reliability,

                CASE
                    WHEN sanitation_division_days IS NOT NULL THEN CONCAT('Sanitation: ', sanitation_division_days)
                    WHEN electrical_district IS NOT NULL THEN 'Electrical: Scheduled Maintenance'
                    ELSE 'Standard Schedule'
                END AS maintenance_schedule,

                CASE
                    WHEN TRY_CAST(electrical_district AS INTEGER) BETWEEN 1 AND 10 THEN 'Newer Infrastructure'
                    WHEN TRY_CAST(electrical_district AS INTEGER) BETWEEN 11 AND 20 THEN 'Mature Infrastructure'
                    WHEN TRY_CAST(electrical_district AS INTEGER) > 20 THEN 'Older Infrastructure'
                    ELSE 'Mixed Age'
                END AS infrastructure_age,

                CASE
                    WHEN electrical_district IS NOT NULL AND electricity_grid IS NOT NULL AND sanitation_division_days IS NOT NULL THEN 'High Capacity'
                    WHEN electrical_district IS NOT NULL AND electricity_grid IS NOT NULL THEN 'Medium Capacity'
                    WHEN electrical_district IS NOT NULL OR sanitation_division_days IS NOT NULL THEN 'Standard Capacity'
                    ELSE 'Limited Capacity'
                END AS capacity_level,

                md5(CONCAT(
                    COALESCE(TRIM(electrical_district),''),'|',
                    COALESCE(TRIM(electricity_grid),''),'|',
                    COALESCE(TRIM(sanitation_division_days),'')
                )) AS infrastructure_hash
            FROM (
                SELECT DISTINCT electrical_district, electricity_grid, sanitation_division_days
                FROM {self.source_table}
                WHERE electrical_district IS NOT NULL OR electricity_grid IS NOT NULL OR sanitation_division_days IS NOT NULL
            ) i
            """,
            "Insert dim_infrastructure_v3",
        )
        self._profile_dim(con, "dim_infrastructure_v3", ["electrical_district", "utility_type"])
        
    # ---------------------------
    # DIM: LOCATION
    # ---------------------------
    def create_dim_location(self, con) -> None:
        logger.info("📍 Building dim_location_v3 ...")

        # Drop existing table
        self._exec(con, "DROP TABLE IF EXISTS dim_location_v3", "Drop dim_location_v3")

        # Create table
        self._exec(
            con,
            """
            CREATE TABLE dim_location_v3 (
                location_id INTEGER PRIMARY KEY,
                street_number VARCHAR,
                street_name VARCHAR,
                street_type VARCHAR,
                street_direction VARCHAR,
                street_address VARCHAR,
                city VARCHAR DEFAULT 'Chicago',
                state VARCHAR DEFAULT 'IL',
                zip_code VARCHAR,
                community_area VARCHAR,
                ward INTEGER,
                police_district VARCHAR,
                police_beat VARCHAR,
                police_sector VARCHAR,
                precinct VARCHAR,
                sanitation_division_days VARCHAR,
                electrical_district VARCHAR,
                electricity_grid VARCHAR,
                latitude DOUBLE,
                longitude DOUBLE,
                x_coordinate DOUBLE,
                y_coordinate DOUBLE,
                location_key VARCHAR UNIQUE,
                address_completeness VARCHAR NOT NULL,
                geographic_region VARCHAR,
                spatial_quality_score DOUBLE DEFAULT 0.0,
                is_geocoded BOOLEAN DEFAULT FALSE,
                coordinate_precision VARCHAR DEFAULT 'Unknown',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            "Create dim_location_v3",
        )

        # Insert deduplicated data
        self._exec(
            con,
            f"""
            WITH cleaned AS (
                SELECT DISTINCT
                    NULLIF(TRIM(street_number), '') AS street_number,
                    NULLIF(TRIM(street_name), '') AS street_name,
                    NULLIF(TRIM(street_type), '') AS street_type,
                    NULLIF(TRIM(street_direction), '') AS street_direction,
                    NULLIF(TRIM(street_address), '') AS street_address,
                    COALESCE(NULLIF(TRIM(city), ''), 'Chicago') AS city,
                    COALESCE(NULLIF(TRIM(state), ''), 'IL') AS state,
                    NULLIF(TRIM(zip_code), '') AS zip_code,
                    NULLIF(TRIM(community_area), '') AS community_area,
                    TRY_CAST(NULLIF(TRIM(ward), '') AS INTEGER) AS ward,
                    NULLIF(TRIM(police_district), '') AS police_district,
                    NULLIF(TRIM(police_beat), '') AS police_beat,
                    NULLIF(TRIM(police_sector), '') AS police_sector,
                    NULLIF(TRIM(precinct), '') AS precinct,
                    NULLIF(TRIM(sanitation_division_days), '') AS sanitation_division_days,
                    NULLIF(TRIM(electrical_district), '') AS electrical_district,
                    NULLIF(TRIM(electricity_grid), '') AS electricity_grid,
                    TRY_CAST(latitude AS DOUBLE) AS latitude,
                    TRY_CAST(longitude AS DOUBLE) AS longitude,
                    TRY_CAST(x_coordinate AS DOUBLE) AS x_coordinate,
                    TRY_CAST(y_coordinate AS DOUBLE) AS y_coordinate
                FROM {self.source_table}
            ),
            hashed AS (
                SELECT *,
                    md5(
                        COALESCE(street_address,'NO_ADDRESS') || '|' ||
                        COALESCE(city,'NO_CITY') || '|' ||
                        COALESCE(zip_code,'NO_ZIP') || '|' ||
                        COALESCE(CAST(latitude AS VARCHAR),'NO_LAT') || '|' ||
                        COALESCE(CAST(longitude AS VARCHAR),'NO_LON') || '|' ||
                        COALESCE(CAST(ward AS VARCHAR),'NO_WARD')
                    ) AS location_hash
                FROM cleaned
            ),
            numbered AS (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY location_hash ORDER BY street_address) AS rn
                FROM hashed
            )
            INSERT INTO dim_location_v3(
                location_id, street_number, street_name, street_type, street_direction, street_address,
                city, state, zip_code, community_area, ward, police_district, police_beat,
                police_sector, precinct, sanitation_division_days, electrical_district, electricity_grid,
                latitude, longitude, x_coordinate, y_coordinate, location_key,
                address_completeness, geographic_region, spatial_quality_score,
                is_geocoded, coordinate_precision
            )
            SELECT
                ROW_NUMBER() OVER (ORDER BY location_hash, rn) AS location_id,
                street_number, street_name, street_type, street_direction, street_address,
                city, state, zip_code, community_area, ward, police_district, police_beat,
                police_sector, precinct, sanitation_division_days, electrical_district, electricity_grid,
                latitude, longitude, x_coordinate, y_coordinate,
                location_hash || '_' || rn AS location_key,
                CASE
                    WHEN street_address IS NOT NULL AND zip_code IS NOT NULL AND latitude IS NOT NULL AND longitude IS NOT NULL THEN 'Complete'
                    WHEN street_address IS NOT NULL AND (zip_code IS NOT NULL OR (latitude IS NOT NULL AND longitude IS NOT NULL)) THEN 'Good'
                    WHEN street_name IS NOT NULL OR latitude IS NOT NULL THEN 'Partial'
                    ELSE 'Poor'
                END AS address_completeness,
                CASE
                    WHEN ward BETWEEN 1 AND 10 THEN 'North Chicago'
                    WHEN ward BETWEEN 11 AND 20 THEN 'Northwest Chicago'
                    WHEN ward BETWEEN 21 AND 30 THEN 'Central Chicago'
                    WHEN ward BETWEEN 31 AND 40 THEN 'Southwest Chicago'
                    WHEN ward BETWEEN 41 AND 50 THEN 'South Chicago'
                    WHEN community_area IS NOT NULL THEN 'Chicago (Community Based)'
                    WHEN latitude BETWEEN 41.6 AND 42.1 AND longitude BETWEEN -87.9 AND -87.5 THEN 'Chicago (Coordinate Based)'
                    ELSE 'Unknown Region'
                END AS geographic_region,
                CASE 
                    WHEN latitude IS NOT NULL AND longitude IS NOT NULL AND street_address IS NOT NULL AND ward IS NOT NULL THEN 1.0
                    WHEN latitude IS NOT NULL AND longitude IS NOT NULL AND street_address IS NOT NULL THEN 0.8
                    WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 0.6
                    WHEN street_address IS NOT NULL AND ward IS NOT NULL THEN 0.4
                    WHEN street_address IS NOT NULL THEN 0.2
                    ELSE 0.0
                END AS spatial_quality_score,
                CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN TRUE ELSE FALSE END AS is_geocoded,
                CASE
                    WHEN latitude IS NOT NULL AND longitude IS NOT NULL AND street_address IS NOT NULL THEN 'Address Level'
                    WHEN latitude IS NOT NULL AND longitude IS NOT NULL AND street_name IS NOT NULL THEN 'Street Level'
                    WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 'Area Level'
                    ELSE 'No Coordinates'
                END AS coordinate_precision
            FROM numbered;
            """,
            "Insert deduplicated dim_location_v3",
        )

        # Profile the dimension
        self._profile_dim(con, "dim_location_v3", ["street_address", "ward"])
  

    # ---------------------------
    # PERFORMANCE INDEXES (best-effort; DuckDB indexes are limited)
    # ---------------------------
    def create_indexes(self, con) -> None:
        logger.info("📊 Creating best-effort indexes (DuckDB) ...")
        idx_sql = [
            ("idx_service_name_v3", "dim_service_v3", "service_name"),
            ("idx_service_category_v3", "dim_service_v3", "service_category"),
            ("idx_service_priority_v3", "dim_service_v3", "priority_level"),
            ("idx_service_emergency_v3", "dim_service_v3", "is_emergency"),

            ("idx_dept_name_v3", "dim_department_v3", "department_name"),
            ("idx_dept_type_v3", "dim_department_v3", "department_type"),

            ("idx_loc_addr_v3", "dim_location_v3", "street_address"),
            ("idx_loc_ward_v3", "dim_location_v3", "ward"),
            ("idx_loc_latlon_v3", "dim_location_v3", "latitude, longitude"),

            ("idx_time_year_month_v3", "dim_time_v3", "year, month"),
            ("idx_time_quarter_v3", "dim_time_v3", "quarter"),

            ("idx_geo_ward_v3", "dim_geography_v3", "ward"),
            ("idx_geo_pd_v3", "dim_geography_v3", "police_district"),

            ("idx_infra_elec_v3", "dim_infrastructure_v3", "electrical_district"),
            ("idx_infra_type_v3", "dim_infrastructure_v3", "utility_type"),
        ]
        ok = 0
        for name, tbl, cols in idx_sql:
            try:
                self._exec(con, f"CREATE INDEX IF NOT EXISTS {name} ON {tbl}({cols})", f"Create {name}")
                ok += 1
            except Exception as e:
                logger.warning(f"⚠️  Could not create index {name} on {tbl}({cols}): {e}")
        logger.info(f"   ✅ Created {ok}/{len(idx_sql)} indexes (as supported by DuckDB).")

    # ---------------------------
    # FACT
    # ---------------------------
    def create_fact(self, con) -> None:
        logger.info("⭐ Building fact_requests_v3 ...")
        self._exec(con, "DROP TABLE IF EXISTS fact_requests_v3", "Drop fact_requests_v3")
        # Join via business keys → surrogate IDs
        self._exec(
            con,
            """
            CREATE TABLE fact_requests_v3 AS
            WITH r AS (
                SELECT * FROM fact_requests_staging_v3
            )
            SELECT
                r.sr_number,
                r.status,
                TRY_CAST(r.created_date AS TIMESTAMP) AS created_ts,
                TRY_CAST(r.last_modified_date AS TIMESTAMP) AS last_modified_ts,
                TRY_CAST(r.closed_date AS TIMESTAMP) AS closed_ts,
                r.duplicate,
                r.legacy_record,
                r.legacy_sr_number,
                r.parent_sr_number,

                s.service_id,
                d.department_id,
                t.date_id AS created_date_id,
                l.location_id,
                g.geography_id,
                i.infrastructure_id

            FROM r
            LEFT JOIN dim_service_v3 s
                ON COALESCE(NULLIF(TRIM(r.sr_type),''),'Unknown Service') = s.service_name
               AND COALESCE(NULLIF(TRIM(r.sr_short_code),''),'UNK') = s.service_short_code
               AND COALESCE(NULLIF(TRIM(r.origin),''),'Unknown Origin') = s.service_origin

            LEFT JOIN dim_department_v3 d
                ON COALESCE(NULLIF(TRIM(r.owner_department),''),'Unknown Department') = d.department_name
               AND COALESCE(NULLIF(TRIM(r.created_department),''),'Unknown Creator') = d.created_department

            LEFT JOIN dim_time_v3 t
                ON CAST(TRY_CAST(r.created_date AS TIMESTAMP) AS DATE) = t.date_id

            LEFT JOIN dim_location_v3 l
                ON md5(CONCAT(
                        COALESCE(TRIM(r.street_address),'NO_ADDRESS'),'|',
                        COALESCE(TRIM(r.city),'NO_CITY'),'|',
                        COALESCE(TRIM(r.zip_code),'NO_ZIP'),'|',
                        COALESCE(CAST(ROUND(TRY_CAST(r.latitude AS DOUBLE),6) AS VARCHAR),'NO_LAT'),'|',
                        COALESCE(CAST(ROUND(TRY_CAST(r.longitude AS DOUBLE),6) AS VARCHAR),'NO_LON'),'|',
                        COALESCE(TRIM(r.ward),'NO_WARD')
                   )) = l.location_key

            LEFT JOIN dim_geography_v3 g
                ON COALESCE(NULLIF(TRIM(r.community_area),''), NULL) IS NOT DISTINCT FROM g.community_area
               AND TRY_CAST(NULLIF(TRIM(r.ward), '') AS INTEGER) IS NOT DISTINCT FROM g.ward
               AND COALESCE(NULLIF(TRIM(r.police_district),''), NULL) IS NOT DISTINCT FROM g.police_district
               AND COALESCE(NULLIF(TRIM(r.police_beat),''), NULL) IS NOT DISTINCT FROM g.police_beat
               AND COALESCE(NULLIF(TRIM(r.police_sector),''), NULL) IS NOT DISTINCT FROM g.police_sector
               AND COALESCE(NULLIF(TRIM(r.precinct),''), NULL) IS NOT DISTINCT FROM g.precinct

            LEFT JOIN dim_infrastructure_v3 i
                ON COALESCE(NULLIF(TRIM(r.electrical_district),''), NULL) IS NOT DISTINCT FROM i.electrical_district
               AND COALESCE(NULLIF(TRIM(r.electricity_grid),''), NULL) IS NOT DISTINCT FROM i.electricity_grid
               AND COALESCE(NULLIF(TRIM(r.sanitation_division_days),''), NULL) IS NOT DISTINCT FROM i.sanitation_division_days
            """,
            "Create fact_requests_v3",
        )
        n = con.execute("SELECT COUNT(*) FROM fact_requests_v3").fetchone()[0]
        logger.info(f"   ✅ fact_requests_v3 rows: {n:,}")

    # ---------------------------
    # DATA QUALITY PROFILE (dimension-level)
    # ---------------------------
    def _profile_dim(self, con, table: str, key_columns: List[str]) -> None:
        try:
            q = {}
            total = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            q["total_records"] = total

            for col in key_columns:
                try:
                    nulls = con.execute(
                        f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL OR TRIM(CAST({col} AS VARCHAR)) = '' OR {col} = 'Unknown'"
                    ).fetchone()[0]
                    q[f"{col}_null_rate"] = (nulls / total) if total else 0.0
                except Exception:
                    q[f"{col}_null_rate"] = "N/A"

            # crude PK duplicate check (assumes {entity}_id naming)
            # grab first integer PK-like column ending with _id
            pk_candidates = [r[0] for r in con.execute(f"PRAGMA table_info('{table}')").fetchall() if r[0].endswith("_id")]
            if pk_candidates:
                pk = pk_candidates[0]
                dups = con.execute(f"SELECT COUNT(*) - COUNT(DISTINCT {pk}) FROM {table}").fetchone()[0]
                q["pk_duplicates"] = int(dups)
            else:
                q["pk_duplicates"] = "N/A"

            self.quality[table] = q
            logger.info(f"   📋 {table} quality: {q}")
        except Exception as e:
            logger.warning(f"⚠️  Quality profiling failed for {table}: {e}")

    # ---------------------------
    # SUMMARY REPORT
    # ---------------------------
    def export_metrics(self, out_path: Path) -> None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w") as f:
            json.dump({"performance": self.performance, "quality": self.quality}, f, indent=2, default=str)
        logger.info(f"📊 Metrics saved → {out_path}")


# ---------------------------
# MAIN ORCHESTRATION
# ---------------------------
if __name__ == "__main__":
    cfg = load_config()
    builder = StarBuilderV3(cfg)

    with duckdb.connect(builder.db_path) as con:
        # sensible execution order
        builder.create_staging(con)
        builder.create_dim_service(con)
        builder.create_dim_department(con)
        builder.create_dim_location(con)
        builder.create_dim_time(con)
        builder.create_dim_geography(con)
        builder.create_dim_infrastructure(con)
        builder.create_indexes(con)
        builder.create_fact(con)

    metrics_file = Path("logs") / f"dimension_metrics_v3_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    builder.export_metrics(metrics_file)
    logger.info("✅ Star schema build (v3) complete. Your original v2 tables remain untouched.")
