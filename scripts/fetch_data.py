import requests
import pandas as pd
import os

def fetch_311_data(start_date, end_date, limit=1000):
    base_url = "https://data.cityofchicago.org/resource/v6vf-nfxy.json"
    where_clause = f"created_date >= '{start_date}T00:00:00.000' AND created_date <= '{end_date}T23:59:59.999'"
    
    params = {
        "$where": where_clause,
        "$limit": limit
    }

    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        data = response.json()
        df = pd.DataFrame(data)
        return df
    else:
        print(f"Failed to fetch data: HTTP {response.status_code}")
        print(response.text)
        return None

if __name__ == "__main__":
    df = fetch_311_data("2021-01-01", "2023-12-31", limit=5000)
    if df is not None and not df.empty:
        # ✅ Make sure data folder exists
        os.makedirs("../data", exist_ok=True)
        
        df.to_csv("./data/chicago_311_data.csv", index=False)
        print("✅ Data fetched and saved to data/chicago_311_data.csv")
    else:
        print("⚠️ No data fetched.")
