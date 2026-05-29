"""Helper functions for bike rental data processing."""

import pandas as pd


def data_to_hourly(data: pd.DataFrame, datetime_col: str) -> pd.DataFrame:
    """Convert the given data to hourly data and return a pandas DataFrame."""
    try:
        data[datetime_col] = pd.to_datetime(data[datetime_col])
        data = (
            data.groupby("location_id").resample("1h", on=datetime_col).size()
        )
        data = data.reset_index(name="count")
        return data
    except Exception as e:
        raise Exception(f"error occurred while converting to hourly data: {e}")


def data_merger(
    data1: pd.DataFrame,
    data2: pd.DataFrame,
    on_cols: list,
    how_to: str,
    suffixe_str: tuple,
) -> pd.DataFrame:
    """Merge the given data and return a pandas DataFrame."""
    try:
        merged_data = data1.merge(
            data2, on=on_cols, how=how_to, suffixes=suffixe_str
        )
        return merged_data
    except Exception as e:
        raise Exception(f"error occurred while merging data: {e}")
