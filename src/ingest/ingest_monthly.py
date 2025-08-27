import requests
import pandas as pd
import datetime
import time
import tomli
from db.duckdb_utils import DuckDBConn, insert_df

# Load config
with open("conf/config.toml", "rb") as f:
    config = tomli.load(f)

API_BASE = config["api"]["base_url"]
BATCH_SIZE = config["api"].get("batch_size", 100000)
APP_TOKEN = config["api"].get("app_token", "")
DB_PATH = config["db"]["path"]

START_YEAR = config["ingestion"]["start_year"]
START_MONTH = config["ingestion"]["start_month"]
END_YEAR = config["ingestion"]["end_year"]
END_MONTH = config["ingestion"]["end_month"]


def fetch_311_data(start_date, end_date, batch_size=BATCH_SIZE):
    offset = 0
    all_rows = []

    while True:
        where_clause = (
            f"created_date >= '{start_date}T00:00:00.000' AND created_date <= '{end_date}T23:59:59.999'"
        )
        params = {
            "$where": where_clause,
            "$limit": batch_size,
            "$offset": offset,
        }
        if APP_TOKEN:
            params["$$app_token"] = APP_TOKEN

        response = requests.get(API_BASE, params=params)
        if response.status_code == 200:
            batch = response.json()
            if not batch:
                break
            all_rows.extend(batch)
            offset += batch_size
            print(f"✅ Fetched {len(batch)} records (offset {offset})")
        else:
            print(f"❌ HTTP {response.status_code} - {response.text}")
            break

        # Avoid rate limit
        time.sleep(0.2)

    return pd.DataFrame(all_rows)


def fetch_all_by_month():
    start_date = datetime.date(START_YEAR, START_MONTH, 1)
    end_date = datetime.date(END_YEAR, END_MONTH, 1)
    current = start_date

    with DuckDBConn(DB_PATH) as con:
        while current <= end_date:
            next_month = (current.replace(day=28) + datetime.timedelta(days=4)).replace(day=1)
            month_end = next_month - datetime.timedelta(days=1)

            print(f"\n⏳ Fetching data from {current} to {month_end}")
            df_month = fetch_311_data(str(current), str(month_end))
            if not df_month.empty:
                insert_df(con, "raw.requests", df_month)
                print(f"✅ Saved {len(df_month)} records to DuckDB")
            else:
                print("⚠️ No data for this month.")

            current = next_month


if __name__ == "__main__":
    fetch_all_by_month()
