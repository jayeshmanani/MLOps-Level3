"""Assets for the bike rental data merging."""

import dagster as dg
import pandas as pd

from bike_rental.defs.assets import constants
from bike_rental.defs.assets.helper import (
    data_merger,
    data_store_csv,
)


@dg.asset(deps=["direct_pick_up_hourly", "bike_rental_hourly"])
def merged_hourly(
    bike_rental_hourly: pd.DataFrame, direct_pick_up_hourly: pd.DataFrame
) -> pd.DataFrame:
    """Merge bike rental hourly data and direct pick up hourly data.

    It reads the two hourly data CSV files, merges them, and
    return the merged data.
    """
    merged_data = data_merger(
        bike_rental_hourly,
        direct_pick_up_hourly,
        on_cols=["datetime", "location_id"],
        how_to="outer",
        suffixe_str=("_rentals", "_pickups"),
    )
    merged_hourly = merged_data.fillna(0)
    # data_store_csv(merged_hourly,
    # constants.F_raw_path.format("merged_hourly"))
    return merged_hourly


@dg.asset(deps=["transform_operation_data", "clean_weather_data"])
def merged_hourly_with_weather(
    transform_operation_data: pd.DataFrame, clean_weather_data: pd.DataFrame
) -> pd.DataFrame:
    """Merge the transformed operation data with weather data.

    It reads the merged hourly data and weather data CSV files, merges
    them, and return the merged data.
    """
    merged_data = data_merger(
        transform_operation_data,
        clean_weather_data,
        on_cols=["datetime"],
        how_to="left",
        suffixe_str=("", "_weather"),
    )
    data_store_csv(
        merged_data, constants.F_raw_path.format("merged_hourly_with_weather")
    )
    return merged_data
