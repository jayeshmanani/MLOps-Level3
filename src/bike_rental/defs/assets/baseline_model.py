"""Baseline model for bike rental prediction."""

import dagster as dg
import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error

from bike_rental.defs.assets.helper import metadata_extractor

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
