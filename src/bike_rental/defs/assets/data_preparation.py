"""prepare the data for Model training and evaluation."""

import dagster as dg
import pandas as pd

from bike_rental.defs.assets.helper import (
    add_lag_features,
    add_rolling_features,
    add_time_based_features,
    metadata_extractor,
)
from bike_rental.defs.resources.csv_io import CSVIO
from bike_rental.defs.resources.project_config import ProjectConfig


def _data_clean_helper(data: pd.DataFrame) -> pd.DataFrame:
    """Clean the curated rental dataset.

    Parameters
    ----------
    data
        Input dataframe to clean.

    Returns
    -------
    pd.DataFrame
        Cleaned dataframe.

    """
    try:
        data = data.copy()
        data["datetime"] = pd.to_datetime(data["datetime"])
        data = data.sort_values("datetime")
        data = data.drop_duplicates()
        data = data.dropna()
        data.fillna(0, inplace=True)
        return data
    except Exception as e:
        raise RuntimeError(f"Error in cleaning curated dataset: {e}")


@dg.asset(deps=["curated_rental_dataset"], group_name="data_preparation")
def clean_curated_data(
    context: dg.AssetExecutionContext,
    csv_io: CSVIO,
    project_config: ProjectConfig,
) -> pd.DataFrame:
    """Clean the curated rental dataset before splitting.

    This includes handling any remaining missing values and ensuring the
    datetime column is in the correct format.
    """
    data = csv_io.read(project_config.curated_path)
    num_rows_before = len(data)
    cols_before = data.columns.tolist()
    data = _data_clean_helper(data)
    cols_after = data.columns.tolist()
    num_rows_after = len(data)
    context.add_output_metadata(
        metadata={
            "num_rows_before": num_rows_before,
            "num_rows_after": num_rows_after,
            "rows_removed": num_rows_before - num_rows_after,
            "cols_before": cols_before,
            "cols_after": cols_after,
            "cols_removed": list(set(cols_before) - set(cols_after)),
        }
        | metadata_extractor(data)
    )
    return data


def _aggregate_hourly(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate hourly data to daily level.

    Parameters
    ----------
    df
        Hourly-level rental dataframe.

    Returns
    -------
    pd.DataFrame
        Hourly-level dataframe with summed counts and averaged features.

    """
    df = df.drop(columns=["location_id", "hour"], errors="ignore").copy()
    daily_df = df.groupby("datetime", as_index=False).agg(
        total_count=("total_count", "sum"),
        count_pickups=("count_pickups", "sum"),
        count_rentals=("count_rentals", "sum"),
        temperature_c=("temperature_c", "mean"),
        perceived_temperature_c=("perceived_temperature_c", "mean"),
        humidity=("humidity", "mean"),
        windspeed_kmh=("windspeed_kmh", "mean"),
        conditions_clear=("conditions_clear", "mean"),
        conditions_clouds=("conditions_clouds", "mean"),
        conditions_heavy_rain=("conditions_heavy_rain", "mean"),
        conditions_light_rain=("conditions_light_rain", "mean"),
        is_holiday=("is_holiday", "max"),
    )
    daily_df = add_time_based_features(daily_df, "datetime")
    return daily_df


def _add_modified_weather_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add lagged weather features based on existing columns.

    Parameters
    ----------
    df
        Input dataframe.

    Returns
    -------
    pd.DataFrame
        Dataframe with added weather lag features.

    """
    df = df.copy()
    weather_cols = [
        "temperature_c",
        "perceived_temperature_c",
        "humidity",
        "windspeed_kmh",
        # "conditions_clear",
        # "conditions_clouds",
        # "conditions_heavy_rain",
        # "conditions_light_rain",
    ]
    for col in weather_cols:
        df = add_lag_features(df, target_col=col, lags=[24])
    return df


@dg.multi_asset(
    outs={
        "train_df": dg.AssetOut(),
        "test_df": dg.AssetOut(),
    },
    deps=["clean_curated_data"],
    group_name="data_preparation",
)
def train_test_split(
    context: dg.AssetExecutionContext,
    clean_curated_data: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split the cleaned curated dataset into train and test sets.

    Parameters
    ----------
    context
        Dagster asset execution context.
    clean_curated_data
        Cleaned dataset to split.

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame]
        Training and testing dataframes.

    """
    data = clean_curated_data.copy()
    data["datetime"] = pd.to_datetime(data["datetime"])
    data = data.sort_values("datetime").reset_index(drop=True)

    # Trying with Removing Location and Daily Prediction
    data = _aggregate_hourly(data)

    # Adding lag and rolling features after aggregation to avoid data leakage
    data = add_time_based_features(data, col="datetime")

    # Adding lag and rolling features after aggregation to avoid data leakage
    data = add_lag_features(data, target_col="total_count", lags=[24, 168])

    data = add_rolling_features(
        data, target_col="total_count", windows=[24, 168]
    )

    # data = _add_modified_weather_features(data)
    data = data.sort_values("datetime").reset_index(drop=True)
    data = data.dropna().reset_index(drop=True)

    # Time ordered split: 70% train, 30% test
    split_index = int(len(data) * 0.7)

    train_df = data.iloc[:split_index]
    test_df = data.iloc[split_index:]

    context.add_output_metadata(
        output_name="train_df", metadata=metadata_extractor(train_df)
    )
    context.add_output_metadata(
        output_name="test_df", metadata=metadata_extractor(test_df)
    )

    return train_df, test_df
