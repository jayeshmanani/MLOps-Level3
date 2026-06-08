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
        # Time-based features
        "dayofweek",
        "year",
        "month",
        "day",
        "quarter",
        "hour",
        "is_month_start",
        "is_month_end",
        "is_weekend",
        "hour_sin",
        "hour_cos",
        # "total_count_lag_24",
        # "total_count_lag_168",
        # "total_count_rolling_mean_24",
        # "total_count_rolling_std_24",
        # "total_count_rolling_mean_168",
        # "total_count_rolling_std_168",
        # Holiday feature
        "is_holiday",
        # Weather features
        # "temperature_c",
        # "perceived_temperature_c",
        # "humidity",
        # "windspeed_kmh",
        # "conditions_clear",
        # "conditions_clouds",
        # "conditions_heavy_rain",
        # "conditions_light_rain",
        # Wearther feature lags
        # "windspeed_kmh_lag_1",
        # "temperature_c_lag_1",
        # "humidity_lag_1",
        # "perceived_temperature_c_lag_1",
        # "conditions_clouds_lag_1",
        # "conditions_clear_lag_1",
        # "conditions_heavy_rain_lag_1",
        # "conditions_light_rain_lag_1",
        # "windspeed_kmh_lag_24",
        # "temperature_c_lag_24",
        # "perceived_temperature_c_lag_24",
        # "humidity_lag_24",
        # "conditions_clear_lag_24",
        # "conditions_clouds_lag_24",
        # "conditions_heavy_rain_lag_24",
        # "conditions_light_rain_lag_24",
        # "windspeed_kmh_lag_168",
        # "temperature_c_lag_168",
        # "perceived_temperature_c_lag_168",
        # "humidity_lag_168",
        # "conditions_light_rain_lag_168",
        # "conditions_heavy_rain_lag_168",
        # "conditions_clouds_lag_168",
        # "conditions_clear_lag_168",
    ]
    TARGET: ClassVar[str] = "total_count"
