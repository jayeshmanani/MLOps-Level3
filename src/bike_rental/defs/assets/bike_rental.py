"""Assets for the bike rental operational data."""

import dagster as dg
import pandas as pd

from bike_rental.defs.assets.helper import data_to_hourly, metadata_extractor


@dg.asset(group_name="operational_data")
def booked_rental_hourly(
    context: dg.AssetExecutionContext,
    raw_booked_rental_data: pd.DataFrame,
) -> pd.DataFrame:
    """Aggregate booked rentals into hourly counts.

    It converts the booked rental records into hourly counts.
    """
    try:
        data = data_to_hourly(raw_booked_rental_data, "datetime")
        context.add_output_metadata(metadata=metadata_extractor(data))
        return data
    except Exception as e:
        raise Exception(
            f"error occurred while converting booked rental data to hourly: {e}"
        )


@dg.asset(group_name="operational_data")
def direct_pickup_hourly(
    context: dg.AssetExecutionContext,
    raw_direct_pickup_data: pd.DataFrame,
) -> pd.DataFrame:
    """Aggregate direct pickups into hourly counts.

    It converts the direct pickup records into hourly counts.
    """
    try:
        data = data_to_hourly(raw_direct_pickup_data, "datetime")
        context.add_output_metadata(metadata=metadata_extractor(data))
        return data
    except Exception as e:
        raise Exception(
            f"error occurred while converting direct pickup data to hourly: {e}"
        )


@dg.asset(group_name="context_data")
def weather_context_data(
    context: dg.AssetExecutionContext,
    raw_weather_data: pd.DataFrame,
) -> pd.DataFrame:
    """Prepare the weather context data.

    It removes unused columns, encodes weather conditions, and normalizes the
    timestamp column.
    """
    try:
        raw_weather_data.drop(columns=["id"], inplace=True)
        raw_weather_data = pd.get_dummies(
            raw_weather_data, columns=["conditions"], dtype=int
        )
        raw_weather_data["datetime"] = pd.to_datetime(
            raw_weather_data["datetime"]
        )
        context.add_output_metadata(
            metadata=metadata_extractor(raw_weather_data)
        )
        return raw_weather_data
    except Exception as e:
        raise Exception(f"error occurred while cleaning weather data: {e}")


@dg.asset(group_name="context_data")
def holiday_context_data(
    context: dg.AssetExecutionContext,
    raw_holiday_data: pd.DataFrame,
) -> pd.DataFrame:
    """Prepare the holiday context data.

    It removes unused columns and normalizes the date column.
    """
    try:
        raw_holiday_data.drop(columns=["id"], inplace=True)
        raw_holiday_data["date"] = pd.to_datetime(raw_holiday_data["date"])
        context.add_output_metadata(
            metadata=metadata_extractor(raw_holiday_data)
        )
        return raw_holiday_data
    except Exception as e:
        raise Exception(f"error occurred while cleaning holiday data: {e}")
