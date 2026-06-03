"""Helper functions for bike rental data processing."""

import numpy as np
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
        df = op_data.copy()
        df[col] = pd.to_datetime(df[col], errors="coerce")
        df["dayofweek"] = df[col].dt.dayofweek
        df["year"] = df[col].dt.year
        df["month"] = df[col].dt.month
        df["day"] = df[col].dt.day
        df["quarter"] = df[col].dt.quarter
        df["date"] = df[col].dt.date
        if col == "datetime":
            df["hour"] = df[col].dt.hour
        df["is_month_start"] = df[col].dt.is_month_start.astype(int)
        df["is_month_end"] = df[col].dt.is_month_end.astype(int)
        df["is_weekend"] = df["dayofweek"].isin([5, 6]).astype(int)
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

        # New features for cyclic encoding of hour
        if "hour" in df.columns:
            df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
            df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24)
        return df
    except Exception as e:
        raise RuntimeError(f"Error in adding time-based features: {e}")


def add_lag_features(
    df: pd.DataFrame, target_col: str, lags: list
) -> pd.DataFrame:
    """Add lag features to the given DataFrame."""
    try:
        df = df.sort_values("datetime")
        for lag in lags:
            df[f"{target_col}_lag_{lag}"] = df[target_col].shift(lag)
        return df
    except Exception as e:
        raise RuntimeError(f"Error in adding lag features: {e}")


def add_rolling_features(
    df: pd.DataFrame, target_col: str, windows: list
) -> pd.DataFrame:
    """Add rolling features to the given DataFrame."""
    try:
        df = df.sort_values("datetime")
        for window in windows:
            df[f"{target_col}_rolling_mean_{window}"] = (
                df[target_col].shift(1).rolling(window=window).mean()
            )

            df[f"{target_col}_rolling_std_{window}"] = (
                df[target_col].shift(1).rolling(window=window).std()
            )

        return df
    except Exception as e:
        raise RuntimeError(f"Error in adding rolling features: {e}")
