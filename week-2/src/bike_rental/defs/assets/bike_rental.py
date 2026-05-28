"""Assets for the bike rental data."""

import dagster as dg
import pandas as pd

from bike_rental.defs.assets import constants
from bike_rental.defs.assets.helper import data_to_hourly
from bike_rental.defs.resources.csv_io import CSVIO


@dg.asset
def bike_rental_hourly(csv_io: CSVIO) -> pd.DataFrame:
    """Asset that imports the bike rental data.

    It reads a CSV file and converts it to hourly data, and
    returns the converted data.
    """
    data = csv_io.read(constants.F_BIKE_RENTALS)
    return data_to_hourly(data, "datetime")


@dg.asset
def direct_pick_up_hourly(csv_io: CSVIO) -> pd.DataFrame:
    """Asset that imports the direct pick up bike rental data.

    It reads a CSV file, converts it to hourly data, and returns the
    converted data.
    """
    data = csv_io.read(constants.F_BIKE_RENTALS_DIRECT_PICKUP)
    return data_to_hourly(data, "datetime")


@dg.asset
def clean_weather_data(csv_io: CSVIO) -> pd.DataFrame:
    """Load and clean the weather data.

    It reads the weather data CSV file, cleans it, and returns the cleaned
    data.
    """
    weather_data = csv_io.read(constants.F_WEATHER)
    weather_data.drop(columns=["id"], inplace=True)
    weather_data = pd.get_dummies(
        weather_data, columns=["conditions"], dtype=int
    )
    weather_data["datetime"] = pd.to_datetime(weather_data["datetime"])
    # data_store_csv(weather_data, constants.F_raw_path.format("weather_data"))
    return weather_data


@dg.asset
def clean_holiday_data(csv_io: CSVIO) -> pd.DataFrame:
    """Load and clean the holiday data.

    It reads the holiday data CSV file, cleans it, and returns the cleaned
    data.
    """
    holiday_data = csv_io.read(constants.F_HOLIDAYS)
    holiday_data.drop(columns=["id"], inplace=True)
    holiday_data["date"] = pd.to_datetime(holiday_data["date"])
    # data_store_csv(holiday_data, constants.F_raw_path.format("holiday_data"))
    return holiday_data
