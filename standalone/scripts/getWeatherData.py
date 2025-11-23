

import argparse
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import requests
import xml.etree.ElementTree as ET
from scripts.utils_data import update_csv_backlog
from scripts.fmi_station_ids import STATION_FMISIDS

FMI_URL = "https://opendata.fmi.fi/wfs"
PARAMS = ["t2m", "ws_10min", "r_1h"]
# Default: Helsinki, Vaasa, Oulu, Pori
DEFAULT_FMISIDS = ",".join([
    STATION_FMISIDS["helsinki"],
    STATION_FMISIDS["vaasa"],
    STATION_FMISIDS["oulu"],
    STATION_FMISIDS["pori"]
])


def fetch_weather_data_window(start: datetime, end: datetime, fmisids: list[str]) -> pd.DataFrame:
    max_days = 7
    all_dfs = []
    for fmisid in fmisids:
        cur = start
        while cur < end:
            chunk_start = cur
            chunk_end = min(chunk_start + timedelta(days=max_days), end)
            params = {
                "service": "WFS",
                "version": "2.0.0",
                "request": "GetFeature",
                "storedquery_id": "fmi::observations::weather::timevaluepair",
                "fmisid": fmisid,
                "parameters": ",".join(PARAMS),
                "starttime": chunk_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "endtime": chunk_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
            print(f"[INFO] Fetching FMI weather for FMISID {fmisid} {params['starttime']} to {params['endtime']} ...")
            resp = requests.get(FMI_URL, params=params, timeout=30)
            try:
                resp.raise_for_status()
                root = ET.fromstring(resp.text)
                ns = {
                    'wml2': 'http://www.opengis.net/waterml/2.0',
                    'gml': 'http://www.opengis.net/gml/3.2'
                }
                param_data = {}
                for param in PARAMS:
                    rows = []
                    for ts in root.findall('.//wml2:MeasurementTimeseries', ns):
                        gid = ts.attrib.get('{http://www.opengis.net/gml/3.2}id', ts.attrib.get('gml:id'))
                        if gid and gid.endswith(f'-{param}'):
                            for pt in ts.findall('wml2:point/wml2:MeasurementTVP', ns):
                                t = pt.find('wml2:time', ns)
                                v = pt.find('wml2:value', ns)
                                if t is not None and v is not None:
                                    rows.append({"timestamp_utc": t.text, param: v.text})
                            break
                    if rows:
                        param_data[param] = pd.DataFrame(rows)
                # Merge all parameter DataFrames on timestamp
                if param_data:
                    df_merged = None
                    for df in param_data.values():
                        if df_merged is None:
                            df_merged = df
                        else:
                            df_merged = pd.merge(df_merged, df, on="timestamp_utc", how="outer")
                    if df_merged is not None:
                        df_merged = df_merged.rename(columns={"t2m": "temperature_C", "ws_10min": "windspeed_ms", "r_1h": "rain_mm"})
                        df_merged["timestamp_utc"] = pd.to_datetime(df_merged["timestamp_utc"], utc=True)
                        df_merged["station"] = fmisid
                        weather_cols = ["temperature_C", "windspeed_ms", "rain_mm"]
                        df_merged = df_merged.dropna(subset=weather_cols, how="all")
                        if not df_merged.empty:
                            all_dfs.append(df_merged)
            except Exception as e:
                print(f"[ERROR] Could not fetch or parse FMI XML for FMISID {fmisid}: {e}")
            cur = chunk_end
    if all_dfs:
        return pd.concat(all_dfs, ignore_index=True).sort_values(["station", "timestamp_utc"]).reset_index(drop=True)
    else:
        return pd.DataFrame(columns=["timestamp_utc", "temperature_C", "windspeed_ms", "rain_mm", "station"])

def main() -> None:

    parser = argparse.ArgumentParser(description="Fetch FMI weather data for a given time window.")
    parser.add_argument("--start", type=str, help="Start datetime (UTC, ISO format)")
    parser.add_argument("--end", type=str, help="End datetime (UTC, ISO format)")
    parser.add_argument("--fmisids", type=str, default=DEFAULT_FMISIDS, help="Comma-separated list of FMISIDs (station IDs)")
    args = parser.parse_args()

    data_dir = Path(__file__).resolve().parent.parent / "data"
    data_dir.mkdir(exist_ok=True)
    out_path = data_dir / "weather.csv"

    # For testing: if no start/end given, use last 2 days
    if args.start and args.end:
        start = datetime.fromisoformat(args.start)
        end = datetime.fromisoformat(args.end)
    else:
        end = datetime.utcnow()
        start = end - timedelta(days=2)
        print(f"[INFO] No start/end given, using last 2 days: {start} to {end}")

    fmisids = [p.strip() for p in args.fmisids.split(",") if p.strip()]
    print(f"[INFO] Fetching weather for explicit window: {start} to {end} for FMISIDs: {fmisids}")
    df_new = fetch_weather_data_window(start, end, fmisids=fmisids)


    if df_new.empty:
        print("No weather data returned for the requested window.")
        return

    print("New weather data batch:")
    print("Shape:", df_new.shape)
    print(df_new.head(20))
    print("df_new columns:", df_new.columns.tolist())

    update_csv_backlog(
        csv_path=out_path,
        df_new=df_new,
        days_backlog=14,
        datetime_col="timestamp_utc",
    )

    print(f"Updated rolling weather backlog in {out_path.resolve()} (14 days of data).")

if __name__ == "__main__":
    main()
