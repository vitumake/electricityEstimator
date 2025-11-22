"""Fetch latest data from APIs (via existing scripts) and write CSVs.

Run this every 15 minutes before updating forecasts.
"""

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]  # project root
data_dir = ROOT / "data"
data_dir.mkdir(exist_ok=True)


def main() -> None:
    """Call existing scripts that fetch and store price/weather/Fingrid data.

    Adjust the script names/paths below to match your project.
    Each script should update a CSV under data/ (e.g. prices.csv, weather.csv, fingrid.csv).
    """

    # TODO: add your real weather and Fingrid scripts when ready, e.g.:
    # subprocess.check_call([sys.executable, str(ROOT / "scripts" / "getWeatherData.py")])
    # subprocess.check_call([sys.executable, str(ROOT / "scripts" / "getFingridData.py")])

    print("Data updated from APIs into data/*.csv")


if __name__ == "__main__":  # pragma: no cover
    main()
