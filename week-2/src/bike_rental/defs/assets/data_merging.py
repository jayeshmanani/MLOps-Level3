"""Assets for the bike rental data merging."""

import dagster as dg
import pandas as pd
from dagster import MaterializeResult, MetadataValue

from bike_rental.defs.assets.helper import (
    data_merger,
)


@dg.asset(
    deps=["direct_pick_up_hourly", "bike_rental_hourly"],
    group_name="operation_data",
)
def merged_hourly(
    bike_rental_hourly: pd.DataFrame, direct_pick_up_hourly: pd.DataFrame
) -> pd.DataFrame:
    """Merge bike rental hourly data and direct pick up hourly data.

    It reads the two hourly data CSV files, merges them, and
    return the merged data.
    """
    merged_data = data_merger(
        bike_rental_hourly,
        direct_pick_up_hourly,
        on_cols=["datetime", "location_id"],
        how_to="outer",
        suffixe_str=("_rentals", "_pickups"),
    )
    merged_hourly = merged_data.fillna(0)
    # csv_io.write(merged_hourly,
    #           project_config.raw_path_template.format("merged_hourly"))
    return MaterializeResult(
        value=merged_hourly,
        metadata={
            "num_rows": MetadataValue.int(len(merged_hourly)),
            "num_columns": MetadataValue.int(len(merged_hourly.columns)),
            "cols_dtypes": MetadataValue.json(
                {col: str(dtype) for col, dtype in merged_hourly.dtypes.items()}
            ),
            "data.head()": MetadataValue.md(merged_hourly.head().to_markdown()),
        },
    )


@dg.asset(
    deps=["transform_operation_data", "clean_weather_data"],
    group_name="weather_data_addition",
)
def weather_enriched_data(
    transform_operation_data: pd.DataFrame, clean_weather_data: pd.DataFrame
) -> pd.DataFrame:
    """Merge the transformed operation data with weather data.

    It reads the merged hourly data and weather data CSV files, merges
    them, and return the merged data.
    """
    merged_data = data_merger(
        transform_operation_data,
        clean_weather_data,
        on_cols=["datetime"],
        how_to="left",
        suffixe_str=("", "_weather"),
    )
    # csv_io.write(merged_data,
    #          project_config.raw_path_template.format("weather_enriched_data"))
    return MaterializeResult(
        value=merged_data,
        metadata={
            "num_rows": MetadataValue.int(len(merged_data)),
            "num_columns": MetadataValue.int(len(merged_data.columns)),
            "cols_dtypes": MetadataValue.json(
                {col: str(dtype) for col, dtype in merged_data.dtypes.items()}
            ),
            "data.head()": MetadataValue.md(merged_data.head().to_markdown()),
        },
    )


@dg.asset(
    deps=["weather_enriched_data", "clean_holiday_data"],
    group_name="holiday_data_addition",
)
def holiday_enriched_data(
    weather_enriched_data: pd.DataFrame, clean_holiday_data: pd.DataFrame
) -> pd.DataFrame:
    """Merge the merged hourly with weather data and holiday data.

    It reads the merged hourly with weather data and holiday data CSV
    files, merges them, and return the merged data.
    """
    merged_data = data_merger(
        weather_enriched_data,
        clean_holiday_data,
        on_cols=["date"],
        how_to="left",
        suffixe_str=(),
    )
    # csv_io.write(merged_data,
    #          project_config.raw_path_template.format("holiday_enriched_data"))
    return MaterializeResult(
        value=merged_data,
        metadata={
            "num_rows": MetadataValue.int(len(merged_data)),
            "num_columns": MetadataValue.int(len(merged_data.columns)),
            "cols_dtypes": MetadataValue.json(
                {col: str(dtype) for col, dtype in merged_data.dtypes.items()}
            ),
            "data.head()": MetadataValue.md(merged_data.head().to_markdown()),
        },
    )
