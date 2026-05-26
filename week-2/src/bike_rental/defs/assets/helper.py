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
    data = data.resample("1h", on=datetime_col).size()
    data = data.reset_index(name="count")
    return data
