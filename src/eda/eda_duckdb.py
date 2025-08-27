import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import os

DB_PATH = "data/chicago_311.duckdb"
OUTPUT_DIR = "eda_outputs_duckdb"

os.makedirs(OUTPUT_DIR, exist_ok=True)

def run_eda():
    con = duckdb.connect(DB_PATH)
    
    print("⏳ Sampling 5 rows to inspect all columns...")
    sample = con.execute("SELECT * FROM raw.requests LIMIT 5").fetchdf()
    with pd.option_context('display.max_columns', None, 'display.width', None):
        print(sample)
        print("\nColumns:", sample.columns)

    # 1️⃣ Total rows
    total_rows = con.execute("SELECT COUNT(*) FROM raw.requests").fetchone()[0]
    print(f"\nTotal rows in raw.requests: {total_rows}")

    # 2️⃣ Missing values
    print("\n⏳ Calculating missing value percentage per column...")
    columns = con.execute("PRAGMA table_info('raw.requests')").fetchdf()['name'].tolist()
    missing_list = []
    for col in columns:
        pct = con.execute(f"""
            SELECT 100.0 * SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) / COUNT(*) AS missing_pct
            FROM raw.requests
        """).fetchone()[0]
        missing_list.append((col, pct))

    missing_pct = pd.DataFrame(missing_list, columns=['column_name', 'missing_pct']).sort_values('missing_pct', ascending=False)
    print(missing_pct)
    missing_pct.to_csv(f"{OUTPUT_DIR}/missing_values_summary.csv", index=False)

    # Create summary table in DuckDB
    con.execute("CREATE OR REPLACE TABLE summary_missing_values AS SELECT * FROM missing_pct")

    # 3️⃣ Duplicate SR numbers
    print("\n⏳ Checking for duplicate SR numbers...")
    dup_check = con.execute("""
        SELECT sr_number, COUNT(*) AS cnt
        FROM raw.requests
        GROUP BY sr_number
        HAVING COUNT(*) > 1
        ORDER BY cnt DESC
        LIMIT 20
    """).fetchdf()
    print("Top duplicate SR numbers:")
    print(dup_check)

    total_dups = con.execute("""
        SELECT COUNT(*) 
        FROM (
            SELECT sr_number
            FROM raw.requests
            GROUP BY sr_number
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    print(f"⚠️ Total duplicated sr_numbers: {total_dups}")

    if total_dups > 0:
        full_dups = con.execute("""
            SELECT sr_number, COUNT(*) AS cnt
            FROM raw.requests
            GROUP BY sr_number
            HAVING COUNT(*) > 1
            ORDER BY cnt DESC
        """).fetchdf()
        full_dups.to_csv(f"{OUTPUT_DIR}/duplicate_sr_numbers.csv", index=False)
        con.execute("CREATE OR REPLACE TABLE summary_duplicate_sr AS SELECT * FROM full_dups")
        print("✅ Saved duplicate SR numbers summary.")

    # 4️⃣ Top complaint types
    print("\n⏳ Fetching top 20 complaint types...")
    top_sr_types = con.execute("""
        SELECT sr_type, COUNT(*) AS cnt
        FROM raw.requests
        GROUP BY sr_type
        ORDER BY cnt DESC
        LIMIT 20
    """).fetchdf()
    print(top_sr_types)

    top_sr_types.to_csv(f"{OUTPUT_DIR}/top_complaint_types.csv", index=False)
    con.execute("CREATE OR REPLACE TABLE summary_top_complaints AS SELECT * FROM top_sr_types")

    # Plot
    fig, ax = plt.subplots(figsize=(10,8))
    bars = ax.barh(top_sr_types['sr_type'], top_sr_types['cnt'], color="skyblue")
    ax.set_title("Top 20 Complaint Types", fontsize=14, weight="bold")
    ax.set_xlabel("Number of Complaints")
    ax.bar_label(bars, fmt="%.0f", padding=3)
    for i, bar in enumerate(bars[:3]):
        bar.set_color("tomato")
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/top_20_complaint_types.png")
    print("✅ Saved top 20 complaint types plot.")

    # 5️⃣ Complaints per month
    print("\n⏳ Aggregating complaints per month...")
    complaints_per_month = con.execute("""
       SELECT STRFTIME('%Y-%m', CAST(created_date AS TIMESTAMP)) AS month, COUNT(*) AS cnt
       FROM raw.requests
       GROUP BY month
       ORDER BY month
    """).fetchdf()
    print(complaints_per_month.head())

    complaints_per_month.to_csv(f"{OUTPUT_DIR}/complaints_per_month.csv", index=False)
    con.execute("CREATE OR REPLACE TABLE summary_complaints_per_month AS SELECT * FROM complaints_per_month")

    plt.figure(figsize=(12,5))
    plt.plot(complaints_per_month['month'], complaints_per_month['cnt'])
    plt.xticks(rotation=45)
    plt.title("Complaints per Month")
    plt.ylabel("Number of Complaints")
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/complaints_per_month.png")
    print("✅ Saved complaints per month plot.")

    # 6️⃣ Closure time analysis
    print("\n⏳ Calculating closure time statistics...")
    closure_stats = con.execute("""
     SELECT
     AVG(EXTRACT(EPOCH FROM (CAST(closed_date AS TIMESTAMP) - CAST(created_date AS TIMESTAMP)))/3600) AS avg_hours,
     MIN(EXTRACT(EPOCH FROM (CAST(closed_date AS TIMESTAMP) - CAST(created_date AS TIMESTAMP)))/3600) AS min_hours,
     MAX(EXTRACT(EPOCH FROM (CAST(closed_date AS TIMESTAMP) - CAST(created_date AS TIMESTAMP)))/3600) AS max_hours
     FROM raw.requests
     WHERE closed_date IS NOT NULL
    """).fetchdf()
    print("Closure time statistics (hours):")
    print(closure_stats)

    closure_stats.to_csv(f"{OUTPUT_DIR}/closure_time_stats.csv", index=False)
    con.execute("CREATE OR REPLACE TABLE summary_closure_stats AS SELECT * FROM closure_stats")

    print("\n⏳ Sampling 500k closure times for histogram...")
    closure_sample = con.execute("""
        SELECT EXTRACT(EPOCH FROM (CAST(closed_date AS TIMESTAMP) - CAST(created_date AS TIMESTAMP)))/3600 AS closure_time
        FROM raw.requests
        WHERE closed_date IS NOT NULL
        LIMIT 500000
    """).fetchdf()
    closure_sample.to_csv(f"{OUTPUT_DIR}/closure_times_sample.csv", index=False)
    con.execute("CREATE OR REPLACE TABLE summary_closure_times AS SELECT * FROM closure_sample")

    plt.figure(figsize=(12,5))
    closure_sample['closure_time'].plot(kind='hist', bins=100, title="Closure Time Distribution (hrs)")
    plt.xlabel("Hours")
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/closure_time_distribution.png")
    print("✅ Saved closure time distribution plot.")

    # 7️⃣ Geo Analysis: Complaints by Zip Code
    print("\n⏳ Aggregating complaints by zip code...")
    complaints_by_zip = con.execute("""
        SELECT
            zip_code,
            COUNT(*) AS cnt
        FROM raw.requests
        WHERE zip_code IS NOT NULL
        GROUP BY zip_code
        ORDER BY cnt DESC
    """).fetchdf()
    print(complaints_by_zip.head())

    complaints_by_zip.to_csv(f"{OUTPUT_DIR}/complaints_by_zip.csv", index=False)
    con.execute("CREATE OR REPLACE TABLE summary_complaints_by_zip AS SELECT * FROM complaints_by_zip")

    # 8️⃣ Geo Analysis: Complaints by Lat/Long (sample 200k for plotting)
    print("\n⏳ Sampling complaints with coordinates...")
    complaints_geo_sample = con.execute("""
        SELECT
            sr_number,
            sr_type,
            created_date,
            latitude,
            longitude
        FROM raw.requests
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        LIMIT 200000
    """).fetchdf()

    complaints_geo_sample.to_csv(f"{OUTPUT_DIR}/complaints_geo_sample.csv", index=False)
    con.execute("CREATE OR REPLACE TABLE summary_complaints_geo AS SELECT * FROM complaints_geo_sample")

    print("✅ Saved geo data for Tableau mapping.")

    print("\n✅ Fast EDA complete! Plots, CSVs & DuckDB summary tables ready for Tableau.")

if __name__ == "__main__":
    run_eda()
