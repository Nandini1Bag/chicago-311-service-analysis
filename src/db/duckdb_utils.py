import duckdb
import os

class DuckDBConn:
    """
    Context manager for DuckDB connection.
    Ensures connection opens and closes cleanly.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.con = None

    def __enter__(self):
        # Create parent directory if missing
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.con = duckdb.connect(self.db_path)
        return self.con

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.con:
            self.con.close()

def ensure_table_from_df(con, table_name: str, df):
    """
    Create a table with the same columns as df if it does not exist.
    Supports schema.table_name.
    """
    if '.' in table_name:
        schema, table = table_name.split('.', 1)
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    else:
        table = table_name

    # Create table with df columns
    con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df LIMIT 0")

    # Add any missing columns
    existing_cols = [r[1] for r in con.execute(f"PRAGMA table_info('{table_name}')").fetchall()]
    for c in df.columns:
        if c not in existing_cols:
            con.execute(f"ALTER TABLE {table_name} ADD COLUMN {c} VARCHAR")


def insert_df(con, table_name: str, df):
    """
    Insert a pandas DataFrame into DuckDB table.
    """
    ensure_table_from_df(con, table_name, df)
    # Align columns
    existing_cols = [r[1] for r in con.execute(f"PRAGMA table_info('{table_name}')").fetchall()]
    for c in existing_cols:
        if c not in df.columns:
            df[c] = None
    df = df[existing_cols]
    # Register temporary table and insert
    con.register("temp_df", df)
    con.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df")
    con.unregister("temp_df")
