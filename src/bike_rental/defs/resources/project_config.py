"""Project configuration resource for file paths and environment settings."""

from typing import Any, ClassVar

import pandas as pd
from dagster import ConfigurableResource
from pydantic import BaseModel, create_model


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
        # "year",
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

    params: dict = {
        "random_forest": {
            "n_estimators": 200,
            "max_depth": None,
            "random_state": 42,
            "n_jobs": -1,
        },
        "xgboost": {
            "n_estimators": 300,
            "learning_rate": 0.05,
            "max_depth": 6,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
            "random_state": 42,
        },
        "linear_regression": {},
    }

    _FeatureModel: ClassVar[type[BaseModel] | None] = None

    @classmethod
    def get_feature_model(cls) -> type[BaseModel]:
        """Return Pydantic model based on FEATURES list."""
        if cls._FeatureModel is None:
            field_definitions: dict[str, tuple] = {
                feature: (float, ...) for feature in cls.FEATURES
            }

            cls._FeatureModel = create_model(
                "FeatureInput", __base__=BaseModel, **field_definitions
            )
        return cls._FeatureModel

    @classmethod
    def features_to_dataframe(
        cls, data: dict[str, Any] | BaseModel
    ) -> pd.DataFrame:
        """Convert dict or FeatureInput to DataFrame."""
        if isinstance(data, BaseModel):
            data = data.model_dump()
        return pd.DataFrame([data])
