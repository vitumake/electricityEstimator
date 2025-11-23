"""Fetch latest data from APIs (via existing scripts) and write CSVs.

Run this every 15 minutes before updating forecasts.
"""



import subprocess
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone
import pandas as pd
import schedule
import time

try:
    from standalone.config import DAYS_BACKLOG, FRESHNESS_HOURS, DAILY_GUARD_HOURS
except ImportError:
    DAYS_BACKLOG = 14
    FRESHNESS_HOURS = 12
    DAILY_GUARD_HOURS = 20

ROOT = Path(__file__).resolve().parent  # standalone root
data_dir = ROOT / "data"
data_dir.mkdir(exist_ok=True)


def get_csv_time_window_and_freshness(csv_path: Path, days_backlog: int = 14, freshness_hours: int = 12) -> tuple[datetime, datetime, bool]:
    """Return (start, end, is_fresh) for the CSV. is_fresh=True if latest data < freshness_hours old."""
    end = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    start = end - timedelta(days=days_backlog)
    is_fresh = False
    is_daily_guard = False
    if csv_path.exists():
        try:
            df = pd.read_csv(csv_path, parse_dates=["timestamp_utc"])
            if not df.empty:
                latest = pd.to_datetime(df["timestamp_utc"]).max()
                if (end - latest) < timedelta(hours=freshness_hours):
                    is_fresh = True
                # Daily guard: only fetch if last data is >DAILY_GUARD_HOURS old
                if (datetime.now(latest.tz) if latest.tzinfo else datetime.now()) - latest < timedelta(hours=DAILY_GUARD_HOURS):
                    is_daily_guard = True
                # If not fresh, only fetch missing up to now
                elif (end - latest) < timedelta(days=days_backlog):
                    start = latest + timedelta(minutes=15)
        except Exception as e:
            print(f"[WARN] Could not parse {csv_path}: {e}")
    return start, end, is_fresh, is_daily_guard

def run_update_once():
    """Run all data update scripts once, passing explicit time windows."""
    

    # Prices (Pörssisähkö)
    price_csv = data_dir / "prices.csv"
    price_start, price_end, price_fresh, price_daily_guard = get_csv_time_window_and_freshness(
        price_csv, days_backlog=DAYS_BACKLOG, freshness_hours=FRESHNESS_HOURS)
    if price_daily_guard:
        print(f"[INFO] Last price data is less than {DAILY_GUARD_HOURS}h old; skipping fetch.")
        latest = None
        if price_csv.exists():
            try:
                df = pd.read_csv(price_csv, parse_dates=["timestamp_utc"])
                if not df.empty:
                    latest = pd.to_datetime(df["timestamp_utc"]).max()
            except Exception as e:
                print(f"[WARN] Could not parse {price_csv}: {e}")
        if latest is not None:
            now = datetime.now(latest.tz) if latest.tzinfo else datetime.now()
            print(f"[DEBUG] Latest price data timestamp: {latest}, now: {now}, delta: {(now - latest).total_seconds()/3600:.2f}h")
    elif price_fresh:
        print(f"[INFO] Price data is fresh (<12h old), skipping fetch.")
    else:
        print(f"[INFO] Price fetch window: {price_start} to {price_end}")
        subprocess.check_call([
            sys.executable, "-m", "scripts.getPriceData",
            "--start", price_start.isoformat(),
            "--end", price_end.isoformat()
        ], cwd=str(ROOT))

    # Fingrid time series
    fingrid_csv = data_dir / "fingrid.csv"
    fingrid_start, fingrid_end, fingrid_fresh, fingrid_daily_guard = get_csv_time_window_and_freshness(
        fingrid_csv, days_backlog=DAYS_BACKLOG, freshness_hours=FRESHNESS_HOURS)
    if fingrid_daily_guard:
        print(f"[INFO] Last Fingrid data is less than {DAILY_GUARD_HOURS}h old; skipping fetch.")
        latest = None
        if fingrid_csv.exists():
            try:
                df = pd.read_csv(fingrid_csv, parse_dates=["timestamp_utc"])
                if not df.empty:
                    latest = pd.to_datetime(df["timestamp_utc"]).max()
            except Exception as e:
                print(f"[WARN] Could not parse {fingrid_csv}: {e}")
        if latest is not None:
            now = datetime.now(latest.tz) if latest.tzinfo else datetime.now()
            print(f"[DEBUG] Latest Fingrid data timestamp: {latest}, now: {now}, delta: {(now - latest).total_seconds()/3600:.2f}h")
    elif fingrid_fresh:
        print(f"[INFO] Fingrid data is fresh (<12h old), skipping fetch.")
    else:
        print(f"[INFO] Fingrid fetch window: {fingrid_start} to {fingrid_end}")
        subprocess.check_call([
            sys.executable, "-m", "scripts.getFingridData",
            "--start", fingrid_start.isoformat(),
            "--end", fingrid_end.isoformat()
        ], cwd=str(ROOT))


    # Weather data (FMI)
    weather_csv = data_dir / "weather.csv"
    weather_start, weather_end, weather_fresh, weather_daily_guard = get_csv_time_window_and_freshness(
        weather_csv, days_backlog=DAYS_BACKLOG, freshness_hours=FRESHNESS_HOURS)
    if weather_daily_guard:
        print(f"[INFO] Last weather data is less than {DAILY_GUARD_HOURS}h old; skipping fetch.")
        latest = None
        if weather_csv.exists():
            try:
                df = pd.read_csv(weather_csv, parse_dates=["timestamp_utc"])
                if not df.empty:
                    latest = pd.to_datetime(df["timestamp_utc"]).max()
            except Exception as e:
                print(f"[WARN] Could not parse {weather_csv}: {e}")
        if latest is not None:
            now = datetime.now(latest.tz) if latest.tzinfo else datetime.now()
            print(f"[DEBUG] Latest weather data timestamp: {latest}, now: {now}, delta: {(now - latest).total_seconds()/3600:.2f}h")
    elif weather_fresh:
        print(f"[INFO] Weather data is fresh (<{FRESHNESS_HOURS}h old), skipping fetch.")
    else:
        print(f"[INFO] Weather fetch window: {weather_start} to {weather_end}")
        subprocess.check_call([
            sys.executable, "-m", "scripts.getWeatherData",
            "--start", weather_start.isoformat(),
            "--end", weather_end.isoformat()
        ], cwd=str(ROOT))

    print("Data updated from APIs into data/*.csv")

def main():
    """Run data update every 12 hours using the schedule library."""
    print("Starting update_data service: will update all data every 12 hours using schedule.")
    schedule.every(12).hours.do(run_update_once)
    # Run once at startup
    run_update_once()
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":  # pragma: no cover
    main()
