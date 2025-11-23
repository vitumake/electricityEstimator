"""Feature-building utilities for the standalone forecaster.

These functions read CSVs in data/ and reproduce the df_model / X
construction used in the notebook.

You still need to fill in build_df_model_from_csv with the exact
feature-engineering code from the notebook (merge, lags, rollings, etc.).
"""

from pathlib import Path
import pandas as pd

DATA_DIR = Path(__file__).parent / "data"


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

    # Drop duplicate index values to avoid resample errors
    prices = prices[~prices.index.duplicated(keep='first')]

    # 1) Resample/align to 15-min grid
    prices_15min = prices.resample("15min").ffill()
    weather_15min = weather.resample("15min").mean().ffill()
    fingrid_15min = fingrid.resample("15min").mean().ffill()

    # 2) Merge into df_full
    df_full = prices_15min.join([weather_15min, fingrid_15min], how="outer")
    print(f"df_full shape after merge: {df_full.shape}")
    # Print summary statistics after feature engineering
    print("\nData summary after feature engineering:")
    desc = df_full.describe(include='all').T
    nan_count = df_full.isna().sum()
    for col in df_full.columns:
        print(f"{col:25} count={desc.at[col, 'count']:.0f} mean={desc.at[col, 'mean'] if 'mean' in desc.columns else 'NA'} std={desc.at[col, 'std'] if 'std' in desc.columns else 'NA'} min={desc.at[col, 'min'] if 'min' in desc.columns else 'NA'} max={desc.at[col, 'max'] if 'max' in desc.columns else 'NA'} NaNs={nan_count[col]}")

    # Rename columns to match model expectations
    rename_map = {
        "price_ct_per_kwh": "price_ct_per_kwh",
        "temperature_C": "temp_avg_c",
        "windspeed_ms": "wind_speed_ms",
        "gen_total_mw": "gen_total_mw",
        "gen_wind_mw": "gen_wind_mw",
        "cons_total_mw": "cons_total_mw",
        # Add more mappings if needed
    }
    df_full = df_full.rename(columns=rename_map)

    # Create price lags 1-4
    if "price_ct_per_kwh" in df_full:
        for lag in range(1, 5):
            df_full[f"price_lag_{lag}"] = df_full["price_ct_per_kwh"].shift(lag)

    # Create other lags (1 and 4) for main features
    for col in ["temp_avg_c", "wind_speed_ms", "rain_mm", "gen_total_mw", "gen_wind_mw", "cons_total_mw"]:
        if col in df_full:
            df_full[f"{col}_lag_1"] = df_full[col].shift(1)
            df_full[f"{col}_lag_4"] = df_full[col].shift(4)

    # Add calendar features
    df_full["dayofweek"] = df_full.index.dayofweek if hasattr(df_full.index, 'dayofweek') else df_full.index.to_series().dt.dayofweek
    df_full["hour"] = df_full.index.hour if hasattr(df_full.index, 'hour') else df_full.index.to_series().dt.hour
    df_full["is_weekend"] = df_full["dayofweek"].isin([5, 6]).astype(int)

    # Create rolling mean features (example: 1h and 4h for price)
    if "price_ct_per_kwh" in df_full:
        df_full["price_roll_mean_1h"] = df_full["price_ct_per_kwh"].rolling(4).mean()
        df_full["price_roll_mean_4h"] = df_full["price_ct_per_kwh"].rolling(16).mean()

    # Target (example: 1h ahead price)
    df_full["target_price_1h_ahead"] = df_full["price_ct_per_kwh"].rolling(4).mean().shift(-4)

    # Return all columns for modeling (dropna at the end)
    df_model = df_full.dropna()
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