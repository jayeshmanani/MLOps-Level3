"""Helper functions for bike rental data processing."""

import pandas as pd


def data_import_csv(file_path: str) -> pd.DataFrame:
    """Import the data from the file path and return a pandas DataFrame."""
    data = pd.DataFrame()
    try:
        data = pd.read_csv(file_path)
    except Exception as e:
        raise Exception(f"error occured data import: {e}")
    return data


def data_store_csv(data: pd.DataFrame, file_path: str) -> None:
    """Store the given data in a CSV file at the given file path."""
    try:
        data.to_csv(file_path, index=False)
    except Exception as e:
        raise Exception(f"error occured data store: {e}")
    return


def data_to_hourly(data: pd.DataFrame, datetime_col: str) -> pd.DataFrame:
    """Convert the given data to hourly data and return a pandas DataFrame."""
    data[datetime_col] = pd.to_datetime(data[datetime_col])
    data = data.groupby("location_id").resample("1h", on=datetime_col).size()
    data = data.reset_index(name="count")
    return data


def data_merger(
    data1: pd.DataFrame,
    data2: pd.DataFrame,
    on_cols: list,
    how_to: str,
    suffixe_str: tuple,
) -> pd.DataFrame:
    """Merge the given data and return a pandas DataFrame."""
    merged_data = data1.merge(
        data2, on=on_cols, how=how_to, suffixes=suffixe_str
    )
    return merged_data
