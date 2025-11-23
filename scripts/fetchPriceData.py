import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

BASE_URL = "https://api.porssisahko.net/v2/price.json"
MAX_WORKERS = 32  # adjust if needed


def fetch_price(ts: datetime) -> tuple[datetime, float]:
    """Fetch price for a single timestamp."""
    iso = ts.isoformat().replace("+00:00", "Z")
    params = {"date": iso}

    try:
        resp = requests.get(BASE_URL, params=params, timeout=10)
    except Exception as e:
        print(f"\n[ERROR] Request failed at {iso}: {e}")
        return ts, float("nan")

    if resp.status_code == 200:
        data = resp.json()
        price = data.get("price")
        return ts, float(price) if price is not None else float("nan")
    elif resp.status_code == 404:
        # No data yet or too far in past/future
        return ts, float("nan")
    else:
        print(f"\n[ERROR] Status {resp.status_code} at {iso}: {resp.text}")
        return ts, float("nan")


def main():
    # Define start and end (UTC)
    start = datetime(2024, 11, 22, 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(2025, 11, 22, 0, 0, 0, tzinfo=timezone.utc)

    step = timedelta(minutes=15)

    print(f"Preparing timestamps from {start} to {end} (UTC)...")

    timestamps = []
    current = start
    while current < end:
        timestamps.append(current)
        current += step

    total = len(timestamps)
    print(f"Total quarter-hour points to fetch: {total}")
    print(f"Using up to {MAX_WORKERS} parallel workers.")
    print("Starting download...")

    results: list[tuple[datetime, float]] = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_ts = {executor.submit(fetch_price, ts): ts for ts in timestamps}

        for future in tqdm(
            as_completed(future_to_ts),
            total=total,
            desc="Fetching prices",
            unit="point",
        ):
            ts, price = future.result()
            results.append((ts, price))

    print("All requests finished, sorting results...")

    # Ensure chronological order
    results.sort(key=lambda x: x[0])

    df_prices = pd.DataFrame(
        {
            "timestamp_utc": [r[0] for r in results],
            "price_ct_per_kwh": [r[1] for r in results],
        }
    )

    print("DataFrame created.")
    print("Shape:", df_prices.shape)
    print(df_prices.head())

    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)

    out_path = data_dir / "data_2024-11-22T0000_2025-11-22T0000.csv"
    df_prices.to_csv(out_path, index=False)
    print(f"Saved CSV to {out_path.resolve()}")


if __name__ == "__main__":
    main()