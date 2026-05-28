"""Project configuration resource for file paths and environment settings."""

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
    raw_path_template: str = "data/raw/raw_{}.csv"
