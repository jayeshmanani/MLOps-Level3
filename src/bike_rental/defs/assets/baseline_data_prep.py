"""Baseline data preparation for bike rental demand forecasting."""

import dagster as dg
import pandas as pd

from bike_rental.defs.assets.helper import metadata_extractor
from bike_rental.defs.resources import CSVIO, ProjectConfig
from lakefs_mod import lfs_conf


def _data_with_location_for_baseline(data: pd.DataFrame) -> pd.DataFrame:
    """Prepare the data with location for baseline modeling."""
    try:
        loc_daily = data[["date", "location_id", "total_count"]].copy()
        loc_daily["date"] = pd.to_datetime(loc_daily["date"])
        loc_daily = (
            loc_daily.groupby(["location_id", "date"])["total_count"]
            .sum()
            .reset_index()
        )
        loc_daily = loc_daily.sort_values(["location_id", "date"]).reset_index(
            drop=True
        )
        loc_daily["naive_pred"] = loc_daily.groupby("location_id")[
            "total_count"
        ].shift(1)
        loc_daily["seasonal_pred_7"] = loc_daily.groupby("location_id")[
            "total_count"
        ].shift(7)
        loc_daily["pred_7day_avg"] = loc_daily.groupby("location_id")[
            "total_count"
        ].transform(lambda s: s.shift(1).rolling(window=7).mean())
        return loc_daily
    except Exception as e:
        raise RuntimeError(f"Error in cleaning curated dataset: {e}")


def _data_without_location_for_baseline(data: pd.DataFrame) -> pd.DataFrame:
    """Prepare the data without location for baseline modeling."""
    try:
        data = data[["date", "total_count"]].copy()
        data["date"] = pd.to_datetime(data["date"])
        data = data.groupby("date")["total_count"].sum().reset_index()
        data = data.sort_values("date").reset_index(drop=True)
        data["naive_pred"] = data["total_count"].shift(1)
        data["seasonal_pred_7"] = data["total_count"].shift(7)
        data["pred_7day_avg"] = (
            data["total_count"].shift(1).rolling(window=7).mean()
        )
        return data
    except Exception as e:
        raise RuntimeError(f"Error in cleaning curated dataset: {e}")


@dg.multi_asset(
    outs={
        "rental_with_loc": dg.AssetOut(),
        "rental_without_loc": dg.AssetOut(),
    },
    group_name="baseline_data_prep",
    deps=["curated_rental_dataset"],
)
def clean_data_for_baseline(
    context: dg.AssetExecutionContext,
    csv_io: CSVIO,
    project_config: ProjectConfig,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Clean the curated rental dataset before for baseline modeling.

    This includes handling any remaining missing values and ensuring the
    datetime column is in the correct format.
    """
    run_id = str(context.run.run_id).split("-")[0]
    data = lfs_conf.read_run_data(
        f_path=project_config.curated_path,
        asset_name="curated_rental_dataset",
        run_id=run_id,
    )
    # data = csv_io.read_csv(project_config.curated_path)
    data = data[["datetime", "total_count", "location_id"]].copy()
    data["date"] = pd.to_datetime(data["datetime"]).dt.date
    rental_with_location = _data_with_location_for_baseline(data)
    rental_without_location = _data_without_location_for_baseline(data)
    context.add_output_metadata(
        output_name="rental_with_loc",
        metadata={
            "rental_with_loc_shape": list(rental_with_location.shape),
            "rental_with_loc_cols": rental_with_location.columns.tolist(),
        }
        | metadata_extractor(rental_with_location),
    )
    context.add_output_metadata(
        output_name="rental_without_loc",
        metadata={
            "rental_without_loc_shape": list(rental_without_location.shape),
            "rental_without_loc_cols": rental_without_location.columns.tolist(),
        }
        | metadata_extractor(rental_without_location),
    )
    return rental_with_location, rental_without_location
