Chicago 311 Data Pipeline (DuckDB)

This project fetches Chicago 311 service request data from the City of Chicago open data API, ingests it into DuckDB, builds a star schema, computes features for modeling, and benchmarks machine learning models on service closure outcomes.

🚀 Features

Incremental ingestion (2018‑12 → 2025‑08) with monthly batching

DuckDB backend (columnar, compressed, analytics‑optimized)

Star schema ETL (dim_service, dim_agency, dim_location, dim_time, fact_requests)

Resumable ingestion via meta.ingestion_log

Feature engineering (closure time, categorical encodings, time splits)

Model benchmarking (LogReg, Random Forest, extensible to XGBoost)

Time‑based train/test splits

Configurable via conf/config.toml

Debug mode for small batch testing

📂 Project Structure
chicago-311-duckdb/
├─ README.md                  
├─ requirements.txt           
├─ .env.example               
├─ conf/
│  └─ config.toml             # API, DB, ETL, ingestion config
├─ data/
│  ├─ chicago_311.duckdb      # DuckDB database file
│  └─ logs/
│     └─ ingest.log           # Optional ingest log
├─ notebooks/
│  └─ 01_eda.ipynb            # Jupyter notebook for exploration
├─ results/
│  └─ benchmarks.csv          # Model benchmark results
├─ src/
│  ├─ db/duckdb_utils.py      # DuckDB helpers
│  ├─ etl/                     # Star schema ETL scripts
│  │  ├─ 01_setup.py
│  │  ├─ 02_dimensions.py
│  │  ├─ 03_fact_prepare.py
│  │  ├─ 04_fact_insert.py
│  │  └─ 05_validate.py
│  ├─ ingest/ingest_monthly.py # Raw data ingestion
│  ├─ features/build_features.py # Feature engineering
│  └─ model/benchmark.py      # Model benchmarking
└─ Makefile                   # Convenience commands

⚙️ Setup
1. Clone & Create Virtual Environment
git clone <repo_url>
cd chicago-311-duckdb
python3 -m venv chicago311_env        # create a unique venv
source chicago311_env/bin/activate    # activate the venv (zsh/macOS)


Windows: chicago311_env\Scripts\activate

Prompt will show (chicago311_env) when active.

2. Install Dependencies
pip install --upgrade pip
pip install -r requirements.txt

3. Configure
cp .env.example .env


Optional: Add CHI_APP_TOKEN (Socrata app token) for higher rate limits

Adjust conf/config.toml for batch sizes, DB path, ETL debug mode, and time ranges

🔄 ETL / Star Schema Usage

Run ETL scripts inside the activated virtual environment:

python src/etl/01_setup.py       # Drop & deduplicate raw requests
python src/etl/02_dimensions.py  # Create & populate dimension tables
python src/etl/03_fact_prepare.py # Prepare fact table with precomputed request_id
python src/etl/04_fact_insert.py # Insert fact records in batches
python src/etl/05_validate.py    # Sanity checks, counts, integrity


Use debug = true in conf/config.toml for small sample runs (~10 rows)

Production run (~12M rows) requires debug = false

Optional: Run all ETL scripts via Makefile

Add this target to your Makefile:

.PHONY: etl-run

etl-run:
	source chicago311_env/bin/activate && \
	pip install -r requirements.txt && \
	python src/etl/01_setup.py && \
	python src/etl/02_dimensions.py && \
	python src/etl/03_fact_prepare.py && \
	python src/etl/04_fact_insert.py && \
	python src/etl/05_validate.py


Run with:

make etl-run


Automatically activates venv, installs dependencies, and runs ETL in order

🔄 Data Ingestion
python -m src.ingest.ingest_monthly --start 2018-12 --end 2025-08


Downloads monthly 311 data

Stores into raw.requests table in DuckDB

Tracks progress in meta.ingestion_log

🛠 Feature Engineering
python -m src.features.build_features --rebuild


Creates feat.requests table

Computes closure time, categorical features, target column

🏋️ Model Benchmarking
python -m src.model.benchmark --train-start 2018-12 --train-end 2023-12 \
                              --test-start 2024-01 --test-end 2025-08


Benchmarks Logistic Regression & Random Forest

Outputs metrics to results/benchmarks.csv

Saves trained models to models/ (if folder exists)

📊 Example Queries (DuckDB CLI / Python)
-- Total requests per month
SELECT _month_id, count(*) FROM raw.requests GROUP BY 1 ORDER BY 1;

-- Fact table join with service dimension
SELECT f.request_id, d.service_name
FROM fact_requests f
JOIN dim_service d ON f.service_id = d.service_id
LIMIT 10;

🧩 Extending

Regression task (predict hours_to_close)

Add more models (XGBoost, LightGBM, neural nets)

Experiment tracking (MLflow / Weights & Biases)

Geospatial enrichment (wards, tracts)

⚠️ Notes

Raw dataset is multi‑GB (~5GB+). DuckDB handles this efficiently

ETL is safe in debug mode; production mode can handle full dataset (~12M rows)

Feature script should be re‑run after new ingestion

Validation script ensures integrity of star schema and helps debug fact insert issues

📜 License

MIT (or your preferred license)