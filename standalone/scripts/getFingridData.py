
# Minimal, clean Fingrid fetcher script
import argparse
from datetime import datetime
from pathlib import Path
from typing import Any
import os
import pandas as pd
import requests
from scripts.utils_data import update_csv_backlog
from scripts.utils_env import load_dotenv

BASE_URL = "https://data.fingrid.fi/api/data"
DATASET_MAP: dict[int, str] = {
    74: "gen_total_mw",
    75: "gen_wind_mw",
    124: "cons_total_mw",
}

def fetch_fingrid_data_window(start: datetime, end: datetime) -> pd.DataFrame:
    load_dotenv()
    api_key = (
        os.getenv("FINGRID_API_KEY_PRIMARY")
        or os.getenv("FINGRID_API_KEY_SECONDARY")
    )
    if not api_key:
        raise RuntimeError("FINGRID_API_KEY_PRIMARY or FINGRID_API_KEY_SECONDARY must be set (e.g. in .env)")
    datasets_param = ",".join(str(ds_id) for ds_id in DATASET_MAP.keys())
    params = {
        "datasets": datasets_param,
        "startTime": start.isoformat().replace("+00:00", "Z"),
        "endTime": end.isoformat().replace("+00:00", "Z"),
        "format": "json",
        "oneRowPerTimePeriod": "true",
        "pageSize": 20000,
        "page": 1,
    }
    headers = {"x-api-key": api_key}
    print(f"[INFO] Fetching Fingrid for {params['startTime']} to {params['endTime']} ...")
    resp = requests.get(BASE_URL, params=params, headers=headers, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    records: list[dict[str, Any]]
    if isinstance(data, dict) and "data" in data and isinstance(data["data"], list):
        records = data["data"]
    elif isinstance(data, list):
        records = data
    else:
        raise ValueError("Unexpected Fingrid response structure")
    if not records:
        cols = ["timestamp_utc", "gen_total_mw", "gen_wind_mw", "cons_total_mw"]
        return pd.DataFrame(columns=cols)
    FIELD_MAP = {
        "Sähköntuotanto Suomessa": "gen_total_mw",
        "Tuulivoimatuotanto - varttitieto": "gen_wind_mw",
    }
    df = pd.DataFrame.from_records(records)
    if "startTime" not in df.columns:
        raise ValueError("Could not find 'startTime' in Fingrid response")
    df["timestamp_utc"] = pd.to_datetime(df["startTime"], utc=True)
    out = pd.DataFrame({"timestamp_utc": df["timestamp_utc"]})
    for fin_key, out_col in FIELD_MAP.items():
        if fin_key in df.columns:
            out[out_col] = pd.to_numeric(df[fin_key], errors="coerce")
        else:
            out[out_col] = float("nan")
    if "cons_total_mw" not in out.columns or out["cons_total_mw"].isna().all():
        kulutus_cols = [c for c in df.columns if "kulutus" in c.lower()]
        if kulutus_cols:
            out["cons_total_mw"] = pd.to_numeric(df[kulutus_cols[0]], errors="coerce")
        elif "dataset_124" in df.columns:
            out["cons_total_mw"] = pd.to_numeric(df["dataset_124"], errors="coerce")
        else:
            out["cons_total_mw"] = float("nan")
    return out

def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch Fingrid data for a given time window.")
    parser.add_argument("--start", type=str, help="Start datetime (UTC, ISO format)")
    parser.add_argument("--end", type=str, help="End datetime (UTC, ISO format)")
    args = parser.parse_args()

    data_dir = Path(__file__).resolve().parent.parent / "data"
    data_dir.mkdir(exist_ok=True)
    out_path = data_dir / "fingrid.csv"

    if args.start and args.end:
        start = datetime.fromisoformat(args.start)
        end = datetime.fromisoformat(args.end)
        print(f"[INFO] Fetching Fingrid for explicit window: {start} to {end}")
        df_new = fetch_fingrid_data_window(start, end)
    else:
        print("[ERROR] --start and --end arguments are required.")
        return

    if df_new.empty:
        print("No Fingrid data returned for the requested window.")
        return

    print("New Fingrid data batch:")
    print("Shape:", df_new.shape)
    print(df_new.head())

    update_csv_backlog(
        csv_path=out_path,
        df_new=df_new,
        days_backlog=14,
        datetime_col="timestamp_utc",
    )

    print(f"Updated rolling Fingrid backlog in {out_path.resolve()} (14 days of data).")

if __name__ == "__main__":
    main()
