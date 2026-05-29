"""Assets for the bike rental data."""

import dagster as dg
import pandas as pd

from bike_rental.defs.assets.helper import data_to_hourly


@dg.asset
def bike_rental_hourly(raw_bike_rental_data: pd.DataFrame) -> pd.DataFrame:
    """Asset that imports the bike rental data.

    It reads a CSV file and converts it to hourly data, and
    returns the converted data.
    """
    try:
        data = data_to_hourly(raw_bike_rental_data, "datetime")
        return data
    except Exception as e:
        raise Exception(
            f"error occurred while converting bike rental data to hourly: {e}"
        )


@dg.asset
def direct_pick_up_hourly(
    raw_direct_pick_up_data: pd.DataFrame,
) -> pd.DataFrame:
    """Asset that imports the direct pick up bike rental data.

    It reads a CSV file, converts it to hourly data, and returns the
    converted data.
    """
    try:
        data = data_to_hourly(raw_direct_pick_up_data, "datetime")
        return data
    except Exception as e:
        raise Exception(
            f"error occurred while converting direct\
                  pick up data to hourly: {e}"
        )


@dg.asset
def clean_weather_data(
    raw_weather_data: pd.DataFrame,
) -> pd.DataFrame:
    """Load and clean the weather data.

    It reads the weather data CSV file, cleans it, and returns the cleaned
    data.
    """
    try:
        raw_weather_data.drop(columns=["id"], inplace=True)
        raw_weather_data = pd.get_dummies(
            raw_weather_data, columns=["conditions"], dtype=int
        )
        raw_weather_data["datetime"] = pd.to_datetime(
            raw_weather_data["datetime"]
        )
        return raw_weather_data
    except Exception as e:
        raise Exception(f"error occurred while cleaning weather data: {e}")


@dg.asset
def clean_holiday_data(raw_holiday_data: pd.DataFrame) -> pd.DataFrame:
    """Load and clean the holiday data.

    It reads the holiday data CSV file, cleans it, and returns the cleaned
    data.
    """
    try:
        raw_holiday_data.drop(columns=["id"], inplace=True)
        raw_holiday_data["date"] = pd.to_datetime(raw_holiday_data["date"])
        return raw_holiday_data
    except Exception as e:
        raise Exception(f"error occurred while cleaning holiday data: {e}")
