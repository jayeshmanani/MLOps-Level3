"""List of all the definitions for the bike rental data."""

from dagster import Definitions, definitions

from bike_rental.defs.assets.bike_rental import (
    bike_rental_hourly,
    clean_holiday_data,
    clean_weather_data,
    direct_pick_up_hourly,
)
from bike_rental.defs.assets.data_merging import (
    merged_hourly,
    merged_with_holiday,
    merged_with_weather,
)
from bike_rental.defs.assets.transformation import (
    final_transformed_data,
    transform_operation_data,
)


@definitions
def defs() -> Definitions:
    """Define the assets for the bike rental data.

    Return a Definitions object containing the assets.
    """
    return Definitions(
        assets=[
            bike_rental_hourly,
            direct_pick_up_hourly,
            merged_hourly,
            clean_weather_data,
            clean_holiday_data,
            merged_with_weather,
            merged_with_holiday,
            transform_operation_data,
            final_transformed_data,
        ]
    )
