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
    """Clean helper function to clean the curated rental dataset."""
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
    context, csv_io: CSVIO, project_config: ProjectConfig
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


def aggregate_hourly(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate hourly data to daily level by summing.

    counts and averaging features.
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


@dg.multi_asset(
    outs={
        "X_train": dg.AssetOut(),
        "X_test": dg.AssetOut(),
        "y_train": dg.AssetOut(),
        "y_test": dg.AssetOut(),
    },
    deps=["clean_curated_data"],
    group_name="data_preparation",
)
def train_test_split(
    context, clean_curated_data: pd.DataFrame, project_config: ProjectConfig
) -> tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
    """Split the cleaned curated dataset into training and testing sets."""
    data = clean_curated_data.copy()
    data["datetime"] = pd.to_datetime(data["datetime"])
    data = data.sort_values("datetime").reset_index(drop=True)

    # Trying with Removing Location and Daily Prediction
    data = aggregate_hourly(data)

    # Adding lag and rolling features after aggregation to avoid data leakage
    data = add_time_based_features(data, col="datetime")

    # Adding lag and rolling features after aggregation to avoid data leakage
    data = add_lag_features(data, target_col="total_count", lags=[1, 24, 168])
    data = add_rolling_features(
        data, target_col="total_count", windows=[24, 168]
    )

    data = data.sort_values("datetime").reset_index(drop=True)
    data = data.dropna().reset_index(drop=True)

    # Time ordered split: 70% train, 30% test
    split_index = int(len(data) * 0.7)

    train = data.iloc[:split_index]
    test = data.iloc[split_index:]

    X_train = train[project_config.FEATURES]
    y_train = train[project_config.TARGET]

    X_test = test[project_config.FEATURES]
    y_test = test[project_config.TARGET]

    context.add_output_metadata(
        output_name="X_train",
        metadata=metadata_extractor(X_train)
        | {"y_train": y_train.head().to_dict()},
    )
    context.add_output_metadata(
        output_name="X_test",
        metadata=metadata_extractor(X_test)
        | {"y_test": y_test.head().to_dict()},
    )

    return X_train, X_test, y_train, y_test
