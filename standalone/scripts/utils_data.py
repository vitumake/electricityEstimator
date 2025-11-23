from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd


def update_csv_backlog(
    csv_path: str | Path,
    df_new: pd.DataFrame,
    days_backlog: int = 14,
    datetime_col: str | None = None,
) -> None:
    """
    Merge newly fetched data into an existing CSV, keeping only `days_backlog`
    days of history and dropping duplicates.

    Parameters
    ----------
    csv_path : str or Path
        Path to the CSV file to update (will be created if missing).
    df_new : pd.DataFrame
        New data to append. Must have a datetime index or a datetime column.
    days_backlog : int
        Number of days of history to retain (default 14).
    datetime_col : str or None
        Name of the datetime column if df_new is not indexed by datetime.
        If None, df_new.index is assumed to be a DatetimeIndex.
    """
    csv_path = Path(csv_path)

    # Ensure datetime index
    if datetime_col is not None:
        df_new = df_new.copy()
        df_new[datetime_col] = pd.to_datetime(df_new[datetime_col])
        df_new = df_new.set_index(datetime_col)
    else:
        if not isinstance(df_new.index, pd.DatetimeIndex):
            raise ValueError("df_new must have a DatetimeIndex or provide datetime_col")

    df_new = df_new.sort_index()

    # Load existing data if present
    if csv_path.exists():
        df_old = pd.read_csv(csv_path, parse_dates=[0], index_col=0)
        df_old = df_old.sort_index()
        df_all = pd.concat([df_old, df_new])
    else:
        df_all = df_new

    # Drop duplicate timestamps (keep latest fetch)
    df_all = df_all[~df_all.index.duplicated(keep="last")]

    # Keep only the last `days_backlog` days
    now = df_all.index.max()
    cutoff = now - timedelta(days=days_backlog)
    df_all = df_all[df_all.index >= cutoff]

    # Save back to CSV
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df_all.to_csv(csv_path)