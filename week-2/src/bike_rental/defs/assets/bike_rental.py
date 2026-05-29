"""Assets for the bike rental data."""

import dagster as dg
import pandas as pd
from dagster import MaterializeResult, MetadataValue

from bike_rental.defs.assets.helper import data_to_hourly


@dg.asset(group_name="hourly_data")
def bike_rental_hourly(raw_bike_rental_data: pd.DataFrame) -> pd.DataFrame:
    """Asset that imports the bike rental data.

    It reads a CSV file and converts it to hourly data, and
    returns the converted data.
    """
    try:
        data = data_to_hourly(raw_bike_rental_data, "datetime")
        return MaterializeResult(
            value=data,
            metadata={
                "num_rows": MetadataValue.int(len(data)),
                "num_columns": MetadataValue.int(len(data.columns)),
                "cols_dtypes": MetadataValue.json(
                    {col: str(dtype) for col, dtype in data.dtypes.items()}
                ),
                "data.head()": MetadataValue.md(data.head().to_markdown()),
            },
        )
    except Exception as e:
        raise Exception(
            f"error occurred while converting bike rental data to hourly: {e}"
        )


@dg.asset(group_name="hourly_data")
def direct_pick_up_hourly(
    raw_direct_pick_up_data: pd.DataFrame,
) -> pd.DataFrame:
    """Asset that imports the direct pick up bike rental data.

    It reads a CSV file, converts it to hourly data, and returns the
    converted data.
    """
    try:
        data = data_to_hourly(raw_direct_pick_up_data, "datetime")
        return MaterializeResult(
            value=data,
            metadata={
                "num_rows": MetadataValue.int(len(data)),
                "num_columns": MetadataValue.int(len(data.columns)),
                "cols_dtypes": MetadataValue.json(
                    {col: str(dtype) for col, dtype in data.dtypes.items()}
                ),
                "data.head()": MetadataValue.md(data.head().to_markdown()),
            },
        )
    except Exception as e:
        raise Exception(
            f"error occurred while converting direct\
                  pick up data to hourly: {e}"
        )


@dg.asset(group_name="weather_data_addition")
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
        return MaterializeResult(
            value=raw_weather_data,
            metadata={
                "num_rows": MetadataValue.int(len(raw_weather_data)),
                "num_columns": MetadataValue.int(len(raw_weather_data.columns)),
                "cols_dtypes": MetadataValue.json(
                    {
                        col: str(dtype)
                        for col, dtype in raw_weather_data.dtypes.items()
                    }
                ),
                "data.head()": MetadataValue.md(
                    raw_weather_data.head().to_markdown()
                ),
            },
        )
    except Exception as e:
        raise Exception(f"error occurred while cleaning weather data: {e}")


@dg.asset(group_name="holiday_data_addition")
def clean_holiday_data(raw_holiday_data: pd.DataFrame) -> pd.DataFrame:
    """Load and clean the holiday data.

    It reads the holiday data CSV file, cleans it, and returns the cleaned
    data.
    """
    try:
        raw_holiday_data.drop(columns=["id"], inplace=True)
        raw_holiday_data["date"] = pd.to_datetime(raw_holiday_data["date"])
        return MaterializeResult(
            value=raw_holiday_data,
            metadata={
                "num_rows": MetadataValue.int(len(raw_holiday_data)),
                "num_columns": MetadataValue.int(len(raw_holiday_data.columns)),
                "cols_dtypes": MetadataValue.json(
                    {
                        col: str(dtype)
                        for col, dtype in raw_holiday_data.dtypes.items()
                    }
                ),
                "data.head()": MetadataValue.md(
                    raw_holiday_data.head().to_markdown()
                ),
            },
        )
    except Exception as e:
        raise Exception(f"error occurred while cleaning holiday data: {e}")
