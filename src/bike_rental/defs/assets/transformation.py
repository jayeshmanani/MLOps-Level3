"""Assets for the final rental feature engineering stage."""

import dagster as dg
import pandas as pd

from bike_rental.defs.assets.helper import (
    add_time_based_features,
    metadata_extractor,
)
from bike_rental.defs.resources.csv_io import CSVIO
from bike_rental.defs.resources.project_config import ProjectConfig


@dg.asset(deps=["operational_rentals_hourly"], group_name="operational_data")
def operational_rental_features(
    context: dg.AssetExecutionContext,
    operational_rentals_hourly: pd.DataFrame,
) -> pd.DataFrame:
    """Add time-based features to the operational rental data.

    This turns the hourly operational totals into a feature table.
    """
    try:
        op_data = add_time_based_features(
            operational_rentals_hourly, "datetime"
        )
        op_data["total_count"] = (
            op_data["count_rentals"] + op_data["count_pickups"]
        )
        context.add_output_metadata(metadata=metadata_extractor(op_data))
        return op_data
    except Exception as e:
        raise RuntimeError(f"Error in transforming operational data: {e}")


@dg.asset(deps=["rentals_with_holidays"], group_name="final_dataset")
def curated_rental_dataset(
    context: dg.AssetExecutionContext,
    csv_io: CSVIO,
    project_config: ProjectConfig,
    rentals_with_holidays: pd.DataFrame,
) -> None:
    """Write the final curated rental dataset to disk."""
    try:
        data = rentals_with_holidays.copy()
        data["is_holiday"] = data["holiday"].notna().astype(int)
        data.drop(columns=["holiday"], inplace=True)
        data.drop(columns=["date"], inplace=True)
        csv_io.write(
            data,
            project_config.curated_path,
        )
        context.add_output_metadata(metadata=metadata_extractor(data))
        return None
    except Exception as e:
        raise RuntimeError(f"Error in writing curated rental dataset: {e}")
