"""Feature-building utilities for the standalone forecaster.

These functions read CSVs in data/ and reproduce the df_model / X
construction used in the notebook.

You still need to fill in build_df_model_from_csv with the exact
feature-engineering code from the notebook (merge, lags, rollings, etc.).
"""

from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"


def load_raw_from_csv() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load raw price, weather and Fingrid data from CSV files in data/.

    Expected files:
      - data/prices.csv
      - data/weather.csv
      - data/fingrid.csv

    The structure should match what the notebook uses.
    """

    prices = pd.read_csv(DATA_DIR / "prices.csv", parse_dates=[0], index_col=0)
    weather = pd.read_csv(DATA_DIR / "weather.csv", parse_dates=[0], index_col=0)
    fingrid = pd.read_csv(DATA_DIR / "fingrid.csv", parse_dates=[0], index_col=0)
    return prices, weather, fingrid


def build_df_model_from_csv() -> pd.DataFrame:
    """Rebuild df_model from CSV inputs, mirroring the notebook pipeline.

    You should copy the code from the notebook that builds df_model
    (merging, target_price_1h_ahead, lag/rolling/calendar features) and
    paste/adapt it inside this function.
    """

    prices, weather, fingrid = load_raw_from_csv()

    # TODO: COPY FEATURE PIPELINE FROM NOTEBOOK HERE
    # Steps typically are:
    #   1) Resample/align prices, weather, fingrid to a common 15-minute grid
    #   2) Merge into df_full
    #   3) Create target_price_1h_ahead (4x15min rolling mean shifted -4)
    #   4) Add lag/rolling features for price, weather, fingrid
    #   5) Add calendar features (hour, weekday, etc.)
    #   6) Select modeling columns -> df_model = df_full[["target_price_1h_ahead", ...feature cols...]]
    #   7) df_model = df_model.dropna()

    raise NotImplementedError(
        "Implement build_df_model_from_csv by copying the df_model construction from the notebook."
    )


def build_next_24_features_from_df_model(df_model: pd.DataFrame) -> pd.DataFrame:
    """Return the last 24 rows of the feature matrix X derived from df_model."""

    if "target_price_1h_ahead" not in df_model.columns:
        raise ValueError("df_model must contain 'target_price_1h_ahead'")

    df_clean = df_model.dropna().copy()
    X = df_clean.drop(columns=["target_price_1h_ahead"])
    if len(X) < 24:
        raise ValueError(f"Need at least 24 rows to build next-24 features, got {len(X)}")
    return X.tail(24)
