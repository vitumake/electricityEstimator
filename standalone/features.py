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

    # 1) Resample/align to 15-min grid
    prices_15min = prices.resample("15T").ffill()
    weather_15min = weather.resample("15T").ffill()
    fingrid_15min = fingrid.resample("15T").ffill()

    # 2) Merge into df_full
    df_full = prices_15min.join([weather_15min, fingrid_15min], how="outer")

    # 3) Create target: 1h ahead price (rolling mean, shifted -4)
    df_full["target_price_1h_ahead"] = df_full["price"].rolling(4).mean().shift(-4)

    # 4) Lag/rolling features (example: price, temperature, windspeed, rain, consumption)
    for col in ["price", "temperature_C", "windspeed_ms", "rain_mm", "consumption_MW"]:
        if col in df_full:
            df_full[f"{col}_lag1"] = df_full[col].shift(1)
            df_full[f"{col}_roll4mean"] = df_full[col].rolling(4).mean()
            df_full[f"{col}_roll12mean"] = df_full[col].rolling(12).mean()

    # 5) Calendar features
    df_full["hour"] = df_full.index.hour
    df_full["weekday"] = df_full.index.weekday
    df_full["month"] = df_full.index.month
    df_full["is_weekend"] = (df_full["weekday"] >= 5).astype(int)

    # 6) Select modeling columns (example: adjust as needed)
    feature_cols = [
        "price", "price_lag1", "price_roll4mean", "price_roll12mean",
        "temperature_C", "temperature_C_lag1", "temperature_C_roll4mean", "temperature_C_roll12mean",
        "windspeed_ms", "windspeed_ms_lag1", "windspeed_ms_roll4mean", "windspeed_ms_roll12mean",
        "rain_mm", "rain_mm_lag1", "rain_mm_roll4mean", "rain_mm_roll12mean",
        "consumption_MW", "consumption_MW_lag1", "consumption_MW_roll4mean", "consumption_MW_roll12mean",
        "hour", "weekday", "month", "is_weekend"
    ]
    # Only keep columns that exist
    feature_cols = [c for c in feature_cols if c in df_full.columns]
    df_model = df_full[["target_price_1h_ahead"] + feature_cols]

    # 7) Drop NA
    df_model = df_model.dropna()
    return df_model


def build_next_24_features_from_df_model(df_model: pd.DataFrame) -> pd.DataFrame:
    """Return the last 24 rows of the feature matrix X derived from df_model."""

    if "target_price_1h_ahead" not in df_model.columns:
        raise ValueError("df_model must contain 'target_price_1h_ahead'")

    df_clean = df_model.dropna().copy()
    X = df_clean.drop(columns=["target_price_1h_ahead"])
    if len(X) < 24:
        raise ValueError(f"Need at least 24 rows to build next-24 features, got {len(X)}")
    return X.tail(24)

def check_feature_compatibility(df_model, gbr_features, nn_features):
    """Prints which features are present/missing for each model."""
    model_cols = set(df_model.columns)
    gbr_set = set(gbr_features)
    nn_set = set(nn_features)
    print("\n[GBR] Features in model but missing in df_model:", sorted(gbr_set - model_cols))
    print("[GBR] Features in df_model but not used by model:", sorted(model_cols - gbr_set))
    print("[NN] Features in model but missing in df_model:", sorted(nn_set - model_cols))
    print("[NN] Features in df_model but not used by model:", sorted(model_cols - nn_set))