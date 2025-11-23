


import argparse
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import requests
import time
from scripts.utils_data import update_csv_backlog

BASE_URL = "https://www.sahkohinta-api.fi/api/vartti/v1/halpa"

def fetch_price_data_window(start: datetime, end: datetime) -> pd.DataFrame:
    """Fetch 15-min price data from sahkohinta-api for a given UTC window, 1 day at a time."""
    all_dfs = []
    cur = start.replace(minute=0, second=0, microsecond=0)
    while cur < end:
        day_start = cur
        day_end = min(day_start + timedelta(days=1), end)
        start_str = day_start.strftime("%Y-%m-%dT%H:%M")
        end_str = day_end.strftime("%Y-%m-%dT%H:%M")
        params = {
            "vartit": 96,
            "tulos": "haja",
            "aikaraja": f"{start_str}_{end_str}",
        }
        print(f"[INFO] Fetching sahkohinta-api for {start_str} to {end_str} ...")
        retry = 0
        while retry < 5:
            try:
                resp = requests.get(BASE_URL, params=params, timeout=20)
                if resp.status_code == 429:
                    wait = 60 * (2 ** retry)  # exponential backoff: 1min, 2min, 4min, 8min, 16min
                    print(f"[WARN] 429 Too Many Requests. Waiting {wait//60} min before retrying...")
                    time.sleep(wait)
                    retry += 1
                    continue
                resp.raise_for_status()
                data = resp.json()
                print(f"[INFO]  - Got {len(data) if data else 0} rows.")
                if data:
                    df = pd.DataFrame(data)
                    df["timestamp_utc"] = pd.to_datetime(df["aikaleima_utc"], utc=True)
                    df["price_ct_per_kwh"] = pd.to_numeric(df["hinta"], errors="coerce")
                    all_dfs.append(df[["timestamp_utc", "price_ct_per_kwh"]])
                break  # success, break retry loop
            except Exception as e:
                print(f"[WARN] Failed to fetch sahkohinta-api for {start_str}: {e}")
                if retry < 4:
                    wait = 60 * (2 ** retry)
                    print(f"[INFO] Retrying in {wait//60} min...")
                    time.sleep(wait)
                retry += 1
        cur = day_end
    if all_dfs:
        return pd.concat(all_dfs, ignore_index=True)
    else:
        return pd.DataFrame(columns=["timestamp_utc", "price_ct_per_kwh"])

def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch price data for a given time window.")
    parser.add_argument("--start", type=str, help="Start datetime (UTC, ISO format)")
    parser.add_argument("--end", type=str, help="End datetime (UTC, ISO format)")
    args = parser.parse_args()


    data_dir = Path(__file__).resolve().parent.parent / "data"
    data_dir.mkdir(exist_ok=True)
    out_path = data_dir / "prices.csv"


    if args.start and args.end:
        start = datetime.fromisoformat(args.start)
        end = datetime.fromisoformat(args.end)
        print(f"[INFO] Fetching prices for explicit window: {start} to {end}")
        df_new = fetch_price_data_window(start, end)
    else:
        print("[ERROR] --start and --end arguments are required.")
        return

    if df_new.empty:
        print("No price data returned for the requested window.")
        return

    print("New price data batch:")
    print("Shape:", df_new.shape)
    print(df_new.head())

    update_csv_backlog(
        csv_path=out_path,
        df_new=df_new,
        days_backlog=14,
        datetime_col="timestamp_utc",
    )

    print(f"Updated rolling price backlog in {out_path.resolve()} (14 days of data).")

if __name__ == "__main__":  # pragma: no cover
    main()