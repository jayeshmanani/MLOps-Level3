"""Baseline model for bike rental prediction."""

import dagster as dg
import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error

from bike_rental.defs.assets.helper import metadata_extractor
from bike_rental.defs.resources.csv_io import CSVIO
from bike_rental.defs.resources.project_config import ProjectConfig

DEFAULT_HOLDOUT_DAYS = 180


def _loc_metrics(df, pred_col):
    """Calculate MAE and RMSE for location-level predictions."""
    df = df.dropna(subset=[pred_col])

    y_true = df["total_count"].values
    y_pred = df[pred_col].values

    mae = mean_absolute_error(y_true, y_pred)
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))

    return mae, rmse


@dg.asset(group_name="baseline_model", deps=["rental_without_loc"])
def base_model_no_loc(context, rental_without_loc) -> None:
    """Generate baseline predictions without location information."""
    data = rental_without_loc.copy()
    data["date"] = pd.to_datetime(data["date"])
    holdout_days = DEFAULT_HOLDOUT_DAYS
    holdout_start = data["date"].max() - pd.Timedelta(days=holdout_days)
    test_daily = data[data["date"] >= holdout_start].copy()

    mae_naive, rmse_naive = _loc_metrics(test_daily, pred_col="naive_pred")

    # Evaluate seasonal (lag-7) on holdout
    mae_seasonal, rmse_seasonal = _loc_metrics(
        test_daily, pred_col="seasonal_pred_7"
    )
    # Evaluate 7-day average on holdout
    mae_7day, rmse_7day = _loc_metrics(test_daily, pred_col="pred_7day_avg")

    eval_df = pd.DataFrame(
        {
            "model": ["naive", "seasonal_lag_7", "7day_avg"],
            "mae": [mae_naive, mae_seasonal, mae_7day],
            "rmse": [rmse_naive, rmse_seasonal, rmse_7day],
        }
    )
    context.add_output_metadata(
        metadata=metadata_extractor(eval_df)
        | {
            "holdout_start": holdout_start.strftime("%Y-%m-%d"),
            "holdout_end": data["date"].max().strftime("%Y-%m-%d"),
            "test_7dayavg": int(test_daily["pred_7day_avg"].sum()),
        }
    )
    return None


@dg.asset(group_name="baseline_model", deps=["rental_with_loc"])
def base_model_with_loc(context, rental_with_loc) -> None:
    """Generate baseline predictions with location information."""
    data = rental_with_loc.copy()
    data["date"] = pd.to_datetime(data["date"])
    holdout_days = DEFAULT_HOLDOUT_DAYS
    holdout_start = data["date"].max() - pd.Timedelta(days=holdout_days)
    test_daily = data[data["date"] >= holdout_start].copy()

    mae_naive, rmse_naive = _loc_metrics(test_daily, pred_col="naive_pred")

    # Evaluate seasonal (lag-7) on holdout
    mae_seasonal, rmse_seasonal = _loc_metrics(
        test_daily, pred_col="seasonal_pred_7"
    )
    # Evaluate 7-day average on holdout
    mae_7day, rmse_7day = _loc_metrics(test_daily, pred_col="pred_7day_avg")

    eval_df = pd.DataFrame(
        {
            "model": ["naive", "seasonal_lag_7", "7day_avg"],
            "mae": [mae_naive, mae_seasonal, mae_7day],
            "rmse": [rmse_naive, rmse_seasonal, rmse_7day],
        }
    )
    context.add_output_metadata(
        metadata=metadata_extractor(eval_df)
        | {
            "holdout_start": holdout_start.strftime("%Y-%m-%d"),
            "holdout_end": data["date"].max().strftime("%Y-%m-%d"),
            "test_7dayavg": int(test_daily["pred_7day_avg"].sum()),
        }
    )
    return None


def _help_prepare_hourly_data(data: pd.DataFrame) -> pd.DataFrame:
    """Help function to prepare hourly data for baseline modeling."""
    try:
        data["datetime"] = pd.to_datetime(data["datetime"])
        hourly_daily = (
            data.groupby("datetime", as_index=False)["total_count"]
            .sum()
            .sort_values("datetime")
            .reset_index(drop=True)
        )

        hourly_daily["naive_pred"] = hourly_daily["total_count"].shift(1)
        hourly_daily["seasonal_pred_24hr"] = hourly_daily["total_count"].shift(
            24
        )
        hourly_daily["pred_24hour_avg"] = (
            hourly_daily["total_count"].shift(1).rolling(24).mean()
        )
        hourly_daily = hourly_daily.dropna()
        hourly_daily = hourly_daily.sort_values("datetime").reset_index(
            drop=True
        )
        hourly_daily["date"] = hourly_daily["datetime"].dt.date
        return hourly_daily
    except Exception as e:
        raise Exception(f"error occurred while preparing hourly data: {e}")


@dg.asset(group_name="baseline_model", deps=["rental_with_loc"])
def base_model_hourly_no_loc(
    context, csv_io: CSVIO, project_config: ProjectConfig
) -> None:
    """Generate baseline predictions hourly without location information."""
    data = csv_io.read(project_config.curated_path)
    data["datetime"] = pd.to_datetime(data["datetime"])
    data = _help_prepare_hourly_data(data)
    holdout_days = DEFAULT_HOLDOUT_DAYS
    holdout_start = data["date"].max() - pd.Timedelta(days=holdout_days)
    hour_test = data[data["date"] >= holdout_start].copy()
    mae_naive, rmse_naive = _loc_metrics(hour_test, pred_col="naive_pred")
    mae_seasonal, rmse_seasonal = _loc_metrics(
        hour_test, pred_col="seasonal_pred_24hr"
    )
    mae_7day, rmse_7day = _loc_metrics(hour_test, pred_col="pred_24hour_avg")
    eval_df = pd.DataFrame(
        {
            "model": ["naive", "seasonal_lag_7", "7day_avg"],
            "mae": [mae_naive, mae_seasonal, mae_7day],
            "rmse": [rmse_naive, rmse_seasonal, rmse_7day],
        }
    )
    context.add_output_metadata(
        metadata=metadata_extractor(eval_df)
        | {
            "holdout_start": holdout_start.strftime("%Y-%m-%d"),
            "holdout_end": data["date"].max().strftime("%Y-%m-%d"),
            "test_7dayavg": int(hour_test["pred_24hour_avg"].sum()),
        }
    )
    return None
