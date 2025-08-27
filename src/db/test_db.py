from db.duckdb_utils import DuckDBConn, insert_df
import pandas as pd

df = pd.DataFrame({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"]
})

with DuckDBConn("data/chicago_311.duckdb") as con:
    insert_df(con, "raw.test_table", df)

print("✅ Data inserted successfully!")
