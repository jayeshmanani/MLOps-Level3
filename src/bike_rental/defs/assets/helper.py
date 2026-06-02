"""Helper functions for bike rental data processing."""

import pandas as pd
from dagster import MetadataValue


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


def metadata_extractor(data: pd.DataFrame) -> dict:
    """Extract metadata from the given data and return a dictionary."""
    try:
        metadata = {
            "num_rows": MetadataValue.int(len(data)),
            "num_columns": MetadataValue.int(len(data.columns)),
            "cols_dtypes": MetadataValue.json(
                {col: str(dtype) for col, dtype in data.dtypes.items()}
            ),
            "data.head()": MetadataValue.md(data.head().to_markdown()),
        }
        return metadata
    except Exception as e:
        raise Exception(f"error occurred while extracting metadata: {e}")


def add_time_based_features(op_data: pd.DataFrame, col: str) -> pd.DataFrame:
    """Add time-based features to the operational rental data."""
    try:
        op_data = op_data.copy()
        op_data[col] = pd.to_datetime(op_data[col], errors="coerce")
        op_data["weekday"] = op_data[col].dt.weekday
        op_data["year"] = op_data[col].dt.year
        op_data["month"] = op_data[col].dt.month
        op_data["day"] = op_data[col].dt.day
        op_data["quarter"] = op_data[col].dt.quarter
        op_data["date"] = op_data[col].dt.date
        if col == "datetime":
            op_data["hour"] = op_data[col].dt.hour
        op_data["is_month_start"] = op_data[col].dt.is_month_start.astype(int)
        op_data["is_month_end"] = op_data[col].dt.is_month_end.astype(int)
        op_data["date"] = pd.to_datetime(op_data["date"], errors="coerce")
        return op_data
    except Exception as e:
        raise RuntimeError(f"Error in adding time-based features: {e}")
