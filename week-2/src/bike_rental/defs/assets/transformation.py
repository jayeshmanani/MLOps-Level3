"""Assets for the bike rental data transformation."""

import dagster as dg
import pandas as pd

from bike_rental.defs.resources.csv_io import CSVIO
from bike_rental.defs.resources.project_config import ProjectConfig


@dg.asset(deps=["merged_hourly"])
def transform_operation_data(merged_hourly: pd.DataFrame) -> pd.DataFrame:
    """Transform the operation data.

    Add new time based features to the operation data, and
    return the transformed data.
    """
    op_data = merged_hourly.copy()
    op_data["total_count"] = op_data["count_rentals"] + op_data["count_pickups"]
    op_data["weekday"] = op_data["datetime"].dt.weekday
    op_data["year"] = op_data["datetime"].dt.year
    op_data["month"] = op_data["datetime"].dt.month
    op_data["day"] = op_data["datetime"].dt.day
    op_data["hour"] = op_data["datetime"].dt.hour
    op_data["quarter"] = op_data["datetime"].dt.quarter
    op_data["date"] = op_data["datetime"].dt.date
    op_data["is_month_start"] = op_data["datetime"].dt.is_month_start.astype(
        int
    )
    op_data["is_month_end"] = op_data["datetime"].dt.is_month_end.astype(int)
    op_data["time_of_day"] = pd.cut(
        op_data["hour"],
        bins=[0, 6, 12, 18, 24],
        labels=["night", "morning", "afternoon", "evening"],
        right=False,
    )
    op_data = pd.get_dummies(op_data, columns=["time_of_day"], dtype=int)
    op_data["date"] = pd.to_datetime(op_data["date"])
    return op_data


@dg.asset(deps=["holiday_enriched_data"])
def final_transformed_data(
    csv_io: CSVIO,
    project_config: ProjectConfig,
    holiday_enriched_data: pd.DataFrame,
) -> None:
    """Transform the merged data with holiday information."""
    final_data = holiday_enriched_data.copy()
    final_data["is_holiday"] = final_data["holiday"].notna().astype(int)
    final_data["holiday_impact"] = final_data.groupby("holiday")[
        "total_count"
    ].transform("mean")
    final_data["holiday_impact"] = final_data["holiday_impact"].fillna(0)
    final_data["deviation_from_normal"] = (
        final_data["total_count"] - final_data["holiday_impact"]
    )
    final_data["deviation_from_normal"] = final_data[
        "deviation_from_normal"
    ].fillna(0)
    final_data.drop(columns=["holiday"], inplace=True)
    final_data.drop(columns=["date"], inplace=True)
    csv_io.write(
        final_data,
        project_config.raw_path_template.format("final_transformed_data"),
    )
    return
