# Electricity Estimator

This project forecasts electricity prices using weather and Fingrid data, with a machine learning pipeline for feature engineering and prediction.

## Structure
- `standalone/` — All scripts, models, and data live here
  - `update_forecast.py` — Main script to generate next-24h price forecasts
  - `features.py` — Feature engineering utilities
  - `models/` — Trained model files
  - `data/` — Input and output CSVs

## Usage
1. Place your input CSVs (`prices.csv`, `weather.csv`, `fingrid.csv`) in `standalone/data/`.
2. Place your trained models in `standalone/models/`.
3. Run the forecast script:
   ```
   python standalone/update_forecast.py
   ```
4. The output will be written to `standalone/data/predictions_next_24h.csv`.

## Requirements
- Python 3.12
- pandas, numpy, scikit-learn, tensorflow, joblib

## Notes
- All paths are relative to the `standalone` folder.
- Make sure your input data covers the same time range for best results.
