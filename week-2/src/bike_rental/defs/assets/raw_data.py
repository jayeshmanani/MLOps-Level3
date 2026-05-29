"""Here module contains the raw data\

definitions for the bike rental project.
"""

import dagster as dg
import pandas as pd
from dagster import MaterializeResult, MetadataValue

from bike_rental.defs.resources.csv_io import CSVIO
from bike_rental.defs.resources.project_config import ProjectConfig


@dg.asset(group_name="raw_data")
def raw_bike_rental_data(
    csv_io: CSVIO, project_config: ProjectConfig
) -> pd.DataFrame:
    """Asset that imports the bike rental data.

    It reads a CSV file and returns the raw data.
    """
    try:
        data = csv_io.read(project_config.f_bike_rentals)
        return MaterializeResult(
            value=data,
            metadata={
                "num_rows": MetadataValue.int(len(data)),
                "num_columns": MetadataValue.int(len(data.columns)),
                "cols_dtypes": MetadataValue.json(
                    {col: str(dtype) for col, dtype in data.dtypes.items()}
                ),
                "data.head()": MetadataValue.md(data.head().to_markdown()),
            },
        )
    except Exception as e:
        raise Exception(f"error occurred while reading bike rental data: {e}")


@dg.asset(group_name="raw_data")
def raw_direct_pick_up_data(
    csv_io: CSVIO, project_config: ProjectConfig
) -> pd.DataFrame:
    """Asset that imports the direct pick up bike rental data.

    It reads a CSV file and returns the raw data.
    """
    try:
        data = csv_io.read(project_config.f_bike_rentals_direct_pickup)
        return MaterializeResult(
            value=data,
            metadata={
                "num_rows": MetadataValue.int(len(data)),
                "num_columns": MetadataValue.int(len(data.columns)),
                "cols_dtypes": MetadataValue.json(
                    {col: str(dtype) for col, dtype in data.dtypes.items()}
                ),
                "data.head()": MetadataValue.md(data.head().to_markdown()),
            },
        )
    except Exception as e:
        raise Exception(
            f"error occurred while reading\
                         direct pick up data: {e}"
        )


@dg.asset(group_name="raw_data")
def raw_weather_data(
    csv_io: CSVIO, project_config: ProjectConfig
) -> pd.DataFrame:
    """Asset that imports the weather data.

    It reads a CSV file and returns the raw data.
    """
    try:
        data = csv_io.read(project_config.f_weather)
        return MaterializeResult(
            value=data,
            metadata={
                "num_rows": MetadataValue.int(len(data)),
                "num_columns": MetadataValue.int(len(data.columns)),
                "cols_dtypes": MetadataValue.json(
                    {col: str(dtype) for col, dtype in data.dtypes.items()}
                ),
                "data.head()": MetadataValue.md(data.head().to_markdown()),
            },
        )
    except Exception as e:
        raise Exception(f"error occurred while reading weather data: {e}")


@dg.asset(group_name="raw_data")
def raw_holiday_data(
    csv_io: CSVIO, project_config: ProjectConfig
) -> pd.DataFrame:
    """Asset that imports the holiday data.

    It reads a CSV file and returns the raw data.
    """
    try:
        data = csv_io.read(project_config.f_holidays)
        return MaterializeResult(
            value=data,
            metadata={
                "num_rows": MetadataValue.int(len(data)),
                "num_columns": MetadataValue.int(len(data.columns)),
                "cols_dtypes": MetadataValue.json(
                    {col: str(dtype) for col, dtype in data.dtypes.items()}
                ),
                "data.head()": MetadataValue.md(data.head().to_markdown()),
            },
        )
    except Exception as e:
        raise Exception(f"error occurred while reading holiday data: {e}")
