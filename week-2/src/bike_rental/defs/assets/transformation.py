"""Assets for the bike rental data transformation."""

import dagster as dg
import pandas as pd


@dg.asset(deps=["merged_hourly"])
def transform_operation_data(merged_hourly: pd.DataFrame) -> pd.DataFrame:
    """Transform the operation data."""
    merged_hourly["total_count"] = (
        merged_hourly["count_rentals"] + merged_hourly["count_pickups"]
    )
    merged_hourly["weekday"] = merged_hourly["datetime"].dt.weekday
    merged_hourly["year"] = merged_hourly["datetime"].dt.year
    merged_hourly["month"] = merged_hourly["datetime"].dt.month
    merged_hourly["day"] = merged_hourly["datetime"].dt.day
    merged_hourly["hour"] = merged_hourly["datetime"].dt.hour
    merged_hourly["quarter"] = merged_hourly["datetime"].dt.quarter
    merged_hourly["is_month_start"] = merged_hourly[
        "datetime"
    ].dt.is_month_start.astype(int)
    merged_hourly["is_month_end"] = merged_hourly[
        "datetime"
    ].dt.is_month_end.astype(int)
    merged_hourly["time_of_day"] = pd.cut(
        merged_hourly["hour"],
        bins=[0, 6, 12, 18, 24],
        labels=["night", "morning", "afternoon", "evening"],
        right=False,
    )
    merged_hourly = pd.get_dummies(
        merged_hourly, columns=["time_of_day"], dtype=int
    )
    # data_store_csv(merged_hourly,
    # constants.F_raw_path.format("operation_data"))
    return merged_hourly
