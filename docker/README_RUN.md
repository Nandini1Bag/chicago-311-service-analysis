# DuckDB Python Docker Container - Run Instructions (DB Inside Container)

This guide explains how to run the DuckDB Python container with the database **already included inside the image**.  
No host folder mount is needed — the database is baked into the container.

---

## **Step 1: Run the Docker Container**

### **Mac / Linux:**
```bash
docker run -it chicago-benchmark-duckdb-python:latest python3
```

### **Windows PowerShell:**
```powershell
docker run -it chicago-benchmark-duckdb-python:latest python3
```

### **Windows CMD:**
```cmd
docker run -it chicago-benchmark-duckdb-python:latest python3
```

**Command explanation:**
- `-it` → interactive terminal
- `python3` → opens Python REPL inside the container

⚠️ **No volume mount needed** - the database is already inside the container!

---

## **Step 2: Connect to DuckDB Inside Python**

Once the Python REPL starts:

```python
import duckdb
import pandas as pd

# Connect to the pre-loaded DuckDB file inside the container
con = duckdb.connect("/data/chicago_311.duckdb")

# List all available tables
print("Available tables:")
tables = con.execute("SHOW TABLES").fetchall()
for table in tables:
    print(f"  - {table[0]}")

# Preview the first table
if tables:
    first_table = tables[0][0]
    print(f"\nPreview of {first_table}:")
    print(con.execute(f"SELECT * FROM {first_table} LIMIT 5").fetchdf())
```

---

## **Step 3: Example Database Operations**

```python
# Get table schema (replace 'your_table_name' with actual table name)
con.execute("DESCRIBE your_table_name").fetchdf()

# Count total records
total_count = con.execute("SELECT COUNT(*) FROM your_table_name").fetchone()[0]
print(f"Total records: {total_count:,}")

# Top 10 most frequent values in a column
result = con.execute("""
    SELECT column_name, COUNT(*) AS count
    FROM your_table_name
    GROUP BY column_name
    ORDER BY count DESC
    LIMIT 10
""").fetchdf()
print(result)

# Convert large result to pandas DataFrame
df = con.execute("SELECT * FROM your_table_name LIMIT 10000").fetchdf()
print(f"DataFrame shape: {df.shape}")
print(df.info())
```

---

## **Step 4: Run Benchmark Script**

If your container includes a benchmark script:

```bash
# Run the benchmark directly
python3 /data/benchmark_etl.py
```

**To save results to your host machine:**

### **Mac / Linux:**
```bash
docker run -it -v $(pwd)/results:/results chicago-benchmark-duckdb-python:latest python3 /data/benchmark_etl.py
```

### **Windows PowerShell:**
```powershell
docker run -it -v ${PWD}\results:/results chicago-benchmark-duckdb-python:latest python3 /data/benchmark_etl.py
```

### **Windows CMD:**
```cmd
docker run -it -v %cd%\results:/results chicago-benchmark-duckdb-python:latest python3 /data/benchmark_etl.py
```

This creates a `results/` folder on your host machine with the benchmark output files.

---

## **Step 5: Interactive Analysis Session**

For extended analysis work, you might want to save your scripts:

```bash
# Mount a host folder for your Python scripts and results
docker run -it -v $(pwd)/workspace:/workspace chicago-benchmark-duckdb-python:latest python3

# Inside Python REPL:
# con = duckdb.connect("/data/chicago_311.duckdb")  # Database inside container
# results.to_csv("/workspace/my_analysis.csv")      # Save to host machine
```

---

## **Step 6: Stop the Container**

1. **Exit Python REPL:**
   ```python
   exit()
   ```

2. **Stop container if still running:**
   ```bash
   docker ps        # find container ID
   docker stop <container_id>
   ```

---

## **Container Management**

```bash
# List running containers
docker ps

# List all containers (including stopped)
docker ps -a

# Remove stopped containers
docker container prune

# Start fresh container (auto-removed after exit)
docker run --rm -it chicago-benchmark-duckdb-python:latest python3

# Check what's inside the container
docker run --rm -it chicago-benchmark-duckdb-python:latest ls -la /data/
```

---

## **Key Differences from External Database Setup**

| Feature | This Setup (DB Inside) | External DB Setup |
|---------|------------------------|-------------------|
| **Database Location** | Inside container (`/data/chicago_311.duckdb`) | Host machine (mounted) |
| **Data Persistence** | Lost when container removed | Persists on host |
| **Portability** | Fully portable image | Requires external DB file |
| **Updates** | Rebuild image for new data | Update host DB file |
| **Best For** | Demos, benchmarks, sharing | Development, production |

---

## **Troubleshooting**

**"No such file or directory" error:**
- Check if database exists: `ls -la /data/` inside container
- Verify the image was built with the database included

**Container starts but no database:**
- The image might not have the database baked in
- Check image build process in README_BUILD.md

**Want to access the database from outside container:**
- This setup doesn't support external access
- Use the external database setup instead (mount host folder)

**Performance seems slow:**
- Increase Docker memory allocation in Docker Desktop settings
- Use `--rm` flag to ensure clean container starts

---

## **Ready for Analysis!**

Your containerized Chicago 311 database is ready! The database is completely self-contained, making it perfect for:

- **Benchmarking** across different machines
- **Sharing** complete datasets with teammates  
- **Demonstrations** without setup complexity
- **Reproducible analysis** environments

🚀 **Start exploring your Chicago 311 data!**