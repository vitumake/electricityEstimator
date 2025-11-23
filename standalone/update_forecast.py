"""
Update next-24h price forecasts and write them to CSV.

Expected artifacts (created by the notebook save cell):
  - models/gbr_model.joblib      (contains {"model": gbr, "feature_cols": [...]})
  - models/nn_preproc.joblib     (contains {"scaler_nn", "feature_cols_nn", "use_log_target"})
  - models/final_nn.keras        (trained Keras model)

Expected data inputs (kept fresh by update_data.py):
  - data/prices.csv
  - data/weather.csv
  - data/fingrid.csv

Output:
  - data/predictions_next_24h.csv
"""

from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from tensorflow import keras

from .features import build_df_model_from_csv, build_next_24_features_from_df_model

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
MODELS_DIR = ROOT / "models"


def load_models():
    gbr_bundle = joblib.load(MODELS_DIR / "gbr_model.joblib")
    gbr = gbr_bundle["model"]
    gbr_features = gbr_bundle["feature_cols"]

    nn_bundle = joblib.load(MODELS_DIR / "nn_preproc.joblib")
    scaler_nn = nn_bundle["scaler_nn"]
    nn_features = nn_bundle["feature_cols_nn"]
    use_log_target = nn_bundle.get("use_log_target", False)

    final_nn = keras.models.load_model(MODELS_DIR / "final_nn.keras")

    return gbr, gbr_features, final_nn, scaler_nn, nn_features, use_log_target


def main() -> pd.DataFrame:
    gbr, gbr_features, final_nn, scaler_nn, nn_features, use_log_target = load_models()

    # 1) Rebuild df_model from latest CSVs
    df_model = build_df_model_from_csv()

    # 2) Last 24 rows as next-24 feature matrix
    X_next24 = build_next_24_features_from_df_model(df_model)

    # 3) Align feature subsets
    X_gbr = X_next24[gbr_features]
    X_nn = X_next24[nn_features]

    # 4) Predict
    gbr_preds = gbr.predict(X_gbr)
    X_nn_scaled = scaler_nn.transform(X_nn)
    nn_preds = final_nn.predict(X_nn_scaled).ravel()
    if use_log_target:
        nn_preds = np.expm1(nn_preds)

    ensemble_preds = 0.5 * gbr_preds + 0.5 * nn_preds

    df_out = pd.DataFrame(
        {
            "timestamp": X_next24.index,
            "gbr_pred": gbr_preds,
            "nn_pred": nn_preds,
            "ensemble_pred": ensemble_preds,
        }
    ).set_index("timestamp")

    DATA_DIR.mkdir(exist_ok=True)
    out_path = DATA_DIR / "predictions_next_24h.csv"
    df_out.to_csv(out_path)
    print(f"Wrote {out_path}")
    return df_out


if __name__ == "__main__":  # pragma: no cover
    main()
