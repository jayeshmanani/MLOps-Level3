"""List of all definitions for the bike rental data pipeline."""

from dagster import Definitions, definitions

from bike_rental.defs.assets.bike_rental import (
    bike_rental_hourly,
    clean_holiday_data,
    clean_weather_data,
    direct_pick_up_hourly,
)
from bike_rental.defs.assets.data_merging import (
    holiday_enriched_data,
    merged_hourly,
    weather_enriched_data,
)
from bike_rental.defs.assets.transformation import (
    final_transformed_data,
    transform_operation_data,
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
            bike_rental_hourly,
            direct_pick_up_hourly,
            merged_hourly,
            clean_weather_data,
            clean_holiday_data,
            weather_enriched_data,
            holiday_enriched_data,
            transform_operation_data,
            final_transformed_data,
        ],
        resources={
            "csv_io": CSVIO(base_path="."),
            "project_config": ProjectConfig(),
        },
    )
