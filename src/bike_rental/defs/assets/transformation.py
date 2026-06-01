"""Assets for the final rental feature engineering stage."""

import dagster as dg
import pandas as pd

from bike_rental.defs.assets.helper import metadata_extractor
from bike_rental.defs.resources.csv_io import CSVIO
from bike_rental.defs.resources.project_config import ProjectConfig


def _add_time_based_features(op_data: pd.DataFrame) -> pd.DataFrame:
    """Add time-based features to the operational rental data."""
    try:
        op_data = op_data.copy()

        op_data["weekday"] = op_data["datetime"].dt.weekday
        op_data["year"] = op_data["datetime"].dt.year
        op_data["month"] = op_data["datetime"].dt.month
        op_data["day"] = op_data["datetime"].dt.day
        op_data["quarter"] = op_data["datetime"].dt.quarter
        op_data["date"] = op_data["datetime"].dt.date
        op_data["is_month_start"] = op_data[
            "datetime"
        ].dt.is_month_start.astype(int)
        op_data["is_month_end"] = op_data["datetime"].dt.is_month_end.astype(
            int
        )
        op_data["date"] = pd.to_datetime(op_data["date"])
        return op_data
    except Exception as e:
        raise RuntimeError(f"Error in adding time-based features: {e}")


@dg.asset(deps=["operational_rentals_hourly"], group_name="operational_data")
def operational_rental_features(
    context, operational_rentals_hourly: pd.DataFrame
) -> pd.DataFrame:
    """Add time-based features to the operational rental data.

    This turns the hourly operational totals into a feature table.
    """
    try:
        op_data = _add_time_based_features(operational_rentals_hourly)
        op_data["total_count"] = (
            op_data["count_rentals"] + op_data["count_pickups"]
        )
        context.add_output_metadata(metadata=metadata_extractor(op_data))
        return op_data
    except Exception as e:
        raise RuntimeError(f"Error in transforming operational data: {e}")


@dg.asset(deps=["rentals_with_holidays"], group_name="final_dataset")
def curated_rental_dataset(
    context,
    csv_io: CSVIO,
    project_config: ProjectConfig,
    rentals_with_holidays: pd.DataFrame,
) -> None:
    """Write the final curated rental dataset to disk."""
    try:
        data = rentals_with_holidays.copy()
        data["is_holiday"] = data["holiday"].notna().astype(int)
        data["holiday_impact"] = data.groupby("holiday")[
            "total_count"
        ].transform("mean")
        data["holiday_impact"] = data["holiday_impact"].fillna(0)
        data["deviation_from_normal"] = (
            data["total_count"] - data["holiday_impact"]
        )
        data["deviation_from_normal"] = data["deviation_from_normal"].fillna(0)
        data.drop(columns=["holiday"], inplace=True)
        data.drop(columns=["date"], inplace=True)
        csv_io.write(
            data,
            project_config.curated_path,
        )
        context.add_output_metadata(metadata=metadata_extractor(data))
        return None
    except Exception as e:
        raise RuntimeError(f"Error in writing curated rental dataset: {e}")
