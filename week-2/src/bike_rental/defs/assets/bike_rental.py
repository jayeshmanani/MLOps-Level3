"""Assets for the bike rental data."""

import dagster as dg
import pandas as pd

from bike_rental.defs.assets import constants
from bike_rental.defs.assets.helper import (
    data_import_csv,
    data_to_hourly,
)


@dg.asset
def bike_rental_hourly() -> pd.DataFrame:
    """Asset that imports the bike rental data.

    It reads a CSV file and converts it to hourly data, and
    returns the converted data.
    """
    data = data_import_csv(constants.F_BIKE_RENTALS)
    # data = data_to_hourly(data, "datetime")
    # data_store_csv(data, constants.F_raw_path.format("bike_rental_hourly"))
    return data_to_hourly(data, "datetime")


@dg.asset
def direct_pick_up_hourly() -> pd.DataFrame:
    """Asset that imports the direct pick up bike rental data.

    It reads a CSV file, converts it to hourly data, and returns the
    converted data.
    """
    data = data_import_csv(constants.F_BIKE_RENTALS_DIRECT_PICKUP)
    # data = data_to_hourly(data, "datetime")
    # data_store_csv(data, constants.F_raw_path.format("direct_pick_up_hourly"))
    return data_to_hourly(data, "datetime")


@dg.asset
def clean_weather_data() -> pd.DataFrame:
    """Load and clean the weather data.

    It reads the weather data CSV file, cleans it, and returns the cleaned
    data.
    """
    weather_data = data_import_csv(constants.F_WEATHER)
    weather_data.drop(columns=["id"], inplace=True)
    weather_data = pd.get_dummies(
        weather_data, columns=["conditions"], dtype=int
    )
    weather_data["datetime"] = pd.to_datetime(weather_data["datetime"])
    # data_store_csv(weather_data, constants.F_raw_path.format("weather_data"))
    return weather_data


@dg.asset
def clean_holiday_data() -> pd.DataFrame:
    """Load and clean the holiday data.

    It reads the holiday data CSV file, cleans it, and returns the cleaned
    data.
    """
    holiday_data = data_import_csv(constants.F_HOLIDAYS)
    holiday_data.drop(columns=["id"], inplace=True)
    holiday_data["date"] = pd.to_datetime(holiday_data["date"])
    # data_store_csv(holiday_data, constants.F_raw_path.format("holiday_data"))
    return holiday_data
