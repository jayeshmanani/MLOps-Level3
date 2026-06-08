"""Raw input data assets for the bike rental project."""

import dagster as dg
import pandas as pd

from bike_rental.defs.assets.helper import metadata_extractor
from bike_rental.defs.resources.csv_io import CSVIO
from bike_rental.defs.resources.project_config import ProjectConfig


@dg.asset(group_name="source_data")
def raw_booked_rental_data(
    context: dg.AssetExecutionContext,
    csv_io: CSVIO,
    project_config: ProjectConfig,
) -> pd.DataFrame:
    """Load the booked rental source data.

    It reads the booked rental CSV file and returns the raw data.
    """
    try:
        data = csv_io.read(project_config.f_bike_rentals)
        context.add_output_metadata(metadata=metadata_extractor(data))
        return data
    except Exception as e:
        raise Exception(f"error occurred while reading bike rental data: {e}")


@dg.asset(group_name="source_data")
def raw_direct_pickup_data(
    context: dg.AssetExecutionContext,
    csv_io: CSVIO,
    project_config: ProjectConfig,
) -> pd.DataFrame:
    """Load the direct pickup source data.

    It reads the direct pickup CSV file and returns the raw data.
    """
    try:
        data = csv_io.read(project_config.f_bike_rentals_direct_pickup)
        context.add_output_metadata(metadata=metadata_extractor(data))
        return data
    except Exception as e:
        raise Exception(f"error occurred while reading direct pickup data: {e}")


@dg.asset(group_name="source_data")
def raw_weather_data(
    context: dg.AssetExecutionContext,
    csv_io: CSVIO,
    project_config: ProjectConfig,
) -> pd.DataFrame:
    """Load the weather source data.

    It reads the weather CSV file and returns the raw data.
    """
    try:
        data = csv_io.read(project_config.f_weather)
        context.add_output_metadata(metadata=metadata_extractor(data))
        return data
    except Exception as e:
        raise Exception(f"error occurred while reading weather data: {e}")


@dg.asset(group_name="source_data")
def raw_holiday_data(
    context: dg.AssetExecutionContext,
    csv_io: CSVIO,
    project_config: ProjectConfig,
) -> pd.DataFrame:
    """Load the holiday source data.

    It reads the holiday CSV file and returns the raw data.
    """
    try:
        data = csv_io.read(project_config.f_holidays)
        context.add_output_metadata(metadata=metadata_extractor(data))
        return data
    except Exception as e:
        raise Exception(f"error occurred while reading holiday data: {e}")
