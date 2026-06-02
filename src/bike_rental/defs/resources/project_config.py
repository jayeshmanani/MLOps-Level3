"""Project configuration resource for file paths and environment settings."""

from typing import ClassVar

from dagster import ConfigurableResource


class ProjectConfig(ConfigurableResource):
    """Holds configurable project-level constants.

    These defaults mirror the previous `constants.py` values but allow
    overriding via resource configuration when needed.
    """

    base_path: str = "."
    f_bike_rentals: str = "data/registered_bike_rentals.csv"
    f_bike_rentals_direct_pickup: str = "data/direct_pickup_bike_rentals.csv"
    f_holidays: str = "data/holidays.csv"
    f_weather: str = "data/weather.csv"
    curated_path: str = "data/raw/curated_rental_dataset.csv"
    raw_path_template: str = "data/raw/raw_{}.csv"
    FEATURES: ClassVar[list[str]] = [
        # "location_id",
        # "weekday",
        # "year",
        # "month",
        # "day",
        # "quarter",
        # "hour",
        "is_month_start",
        "is_month_end",
        "temperature_c",
        "perceived_temperature_c",
        "humidity",
        "windspeed_kmh",
        "conditions_clear",
        "conditions_clouds",
        "conditions_heavy_rain",
        "conditions_light_rain",
        "is_holiday",
    ]
    TARGET: ClassVar[str] = "total_count"
