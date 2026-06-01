"""Assets for combining the bike rental data sources."""

import dagster as dg
import pandas as pd

from bike_rental.defs.assets.helper import data_merger, metadata_extractor


@dg.asset(
    deps=["direct_pickup_hourly", "booked_rental_hourly"],
    group_name="operational_data",
)
def operational_rentals_hourly(
    context,
    booked_rental_hourly: pd.DataFrame,
    direct_pickup_hourly: pd.DataFrame,
) -> pd.DataFrame:
    """Combine booked rentals and direct pickups into one hourly table.

    The resulting table represents total operational rental activity by hour.
    """
    try:
        merged_data = data_merger(
            booked_rental_hourly,
            direct_pickup_hourly,
            on_cols=["datetime", "location_id"],
            how_to="outer",
            suffixe_str=("_rentals", "_pickups"),
        )
        operational_hourly = merged_data.fillna(0)
        context.add_output_metadata(
            metadata=metadata_extractor(operational_hourly)
        )
        return operational_hourly
    except Exception as e:
        raise Exception(f"error occurred while merging hourly data: {e}")


@dg.asset(
    deps=["operational_rental_features", "weather_context_data"],
    group_name="context_data",
)
def rentals_with_weather(
    context,
    operational_rental_features: pd.DataFrame,
    weather_context_data: pd.DataFrame,
) -> pd.DataFrame:
    """Join rental features with weather context data.

    The output keeps the operational features while adding weather fields.
    """
    try:
        merged_data = data_merger(
            operational_rental_features,
            weather_context_data,
            on_cols=["datetime"],
            how_to="left",
            suffixe_str=("", "_weather"),
        )
        context.add_output_metadata(metadata=metadata_extractor(merged_data))
        return merged_data
    except Exception as e:
        raise Exception(f"error occurred while merging weather data: {e}")


@dg.asset(
    deps=["rentals_with_weather", "holiday_context_data"],
    group_name="context_data",
)
def rentals_with_holidays(
    context,
    rentals_with_weather: pd.DataFrame,
    holiday_context_data: pd.DataFrame,
) -> pd.DataFrame:
    """Join rental features with holiday context data.

    The output keeps the weather-enriched rental features and adds holidays.
    """
    try:
        merged_data = data_merger(
            rentals_with_weather,
            holiday_context_data,
            on_cols=["date"],
            how_to="left",
            suffixe_str=(),
        )
        context.add_output_metadata(metadata=metadata_extractor(merged_data))
        return merged_data
    except Exception as e:
        raise Exception(f"error occurred while merging holiday data: {e}")
