"""Dagster definitions for the bike rental data pipeline."""

from dagster import Definitions, definitions

from bike_rental.defs.assets.bike_rental import (
    booked_rental_hourly,
    direct_pickup_hourly,
    holiday_context_data,
    weather_context_data,
)
from bike_rental.defs.assets.data_merging import (
    operational_rentals_hourly,
    rentals_with_holidays,
    rentals_with_weather,
)
from bike_rental.defs.assets.raw_data import (
    raw_booked_rental_data,
    raw_direct_pickup_data,
    raw_holiday_data,
    raw_weather_data,
)
from bike_rental.defs.assets.transformation import (
    curated_rental_dataset,
    operational_rental_features,
)
from bike_rental.defs.resources.csv_io import CSVIO
from bike_rental.defs.resources.project_config import ProjectConfig


@definitions
def defs() -> Definitions:
    """Define the assets for the bike rental data.

    Return a Definitions object containing the assets.
    """
    return Definitions(
        assets=[
            raw_booked_rental_data,
            raw_direct_pickup_data,
            raw_holiday_data,
            raw_weather_data,
            booked_rental_hourly,
            direct_pickup_hourly,
            operational_rentals_hourly,
            weather_context_data,
            holiday_context_data,
            operational_rental_features,
            rentals_with_weather,
            rentals_with_holidays,
            curated_rental_dataset,
        ],
        resources={
            "csv_io": CSVIO(base_path="."),
            "project_config": ProjectConfig(),
        },
    )
