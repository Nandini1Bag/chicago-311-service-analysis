import time
import pandas as pd
import duckdb

# -----------------------------
# CONFIGURATION
# -----------------------------
DB_CONFIG = {
    "duckdb": {"path": "data/chicago_311.duckdb"},
    # "postgres": {...}  # Commented out for now
    # "mongodb": {...}  # Commented out for now
}

# -----------------------------
# QUERY DEFINITIONS
# -----------------------------
QUERIES = {
    "count_requests_per_service": """
        SELECT service_id, COUNT(*) AS cnt
        FROM fact_requests_v3
        GROUP BY service_id;
    """,
    "top_10_services": """
        SELECT service_id, COUNT(*) AS cnt
        FROM fact_requests_v3
        GROUP BY service_id
        ORDER BY cnt DESC
        LIMIT 10;
    """,
    "requests_per_department": """
        SELECT department_id, COUNT(*) AS cnt
        FROM fact_requests_v3
        GROUP BY department_id;
    """,
    "requests_per_location": """
        SELECT location_id, COUNT(*) AS cnt
        FROM fact_requests_v3
        GROUP BY location_id
        ORDER BY cnt DESC
        LIMIT 10;
    """,
    "requests_by_month": """
        SELECT STRFTIME('%Y-%m', date_id) AS month, COUNT(*) AS cnt
        FROM fact_requests_v3
        GROUP BY month
        ORDER BY month;
    """,
    "join_service_department": """
        SELECT s.service_name, d.department_name, COUNT(*) AS cnt
        FROM fact_requests_v3 f
        LEFT JOIN dim_service_v3 s ON f.service_id = s.service_id
        LEFT JOIN dim_department_v3 d ON f.department_id = d.department_id
        GROUP BY s.service_name, d.department_name
        ORDER BY cnt DESC
        LIMIT 10;
    """
}

# -----------------------------
# BENCHMARK FUNCTION
# -----------------------------
def benchmark_duckdb(conn, queries):
    records = []
    for name, sql in queries.items():
        start = time.perf_counter()
        df = conn.execute(sql).fetchdf()
        elapsed = time.perf_counter() - start
        rows = len(df)
        print(f"[DuckDB] {name}: {elapsed:.4f}s, rows: {rows}")
        records.append({
            "DB": "DuckDB",
            "Query": name,
            "Execution_Time_sec": float(elapsed),
            "Rows": rows
        })
    return records

# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    # Connect to DuckDB
    duck = duckdb.connect(DB_CONFIG["duckdb"]["path"])
    duck_records = benchmark_duckdb(duck, QUERIES)

    # Combine results into a DataFrame
    df = pd.DataFrame(duck_records)

    # Ensure numeric type for Tableau
    df["Execution_Time_sec"] = pd.to_numeric(df["Execution_Time_sec"])
    df["Rows"] = pd.to_numeric(df["Rows"])

    print("\nBenchmark Results (DuckDB only):")
    print(df)

    # Save long-format CSV ready for Tableau
    df.to_csv("benchmark_results_duckdb_long.csv", index=False)
    print("✅ CSV saved: benchmark_results_duckdb_long.csv")
