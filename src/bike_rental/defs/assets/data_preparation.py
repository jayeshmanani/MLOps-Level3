"""prepare the data for Model training and evaluation."""

import dagster as dg
import pandas as pd

from bike_rental.defs.assets.helper import metadata_extractor
from bike_rental.defs.resources.csv_io import CSVIO
from bike_rental.defs.resources.project_config import ProjectConfig


def _data_clean_helper(data: pd.DataFrame) -> pd.DataFrame:
    """Clean helper function to clean the curated rental dataset."""
    try:
        data = data.copy()
        data["datetime"] = pd.to_datetime(data["datetime"])
        data = data.sort_values("datetime")
        data = data.drop_duplicates()
        data = data.drop(columns=["count_rentals", "count_pickups"])
        data = data.dropna()
        data.fillna(0, inplace=True)
        return data
    except Exception as e:
        raise RuntimeError(f"Error in cleaning curated dataset: {e}")


@dg.asset(deps=["curated_rental_dataset"], group_name="data_preparation")
def clean_curated_data(
    context, csv_io: CSVIO, project_config: ProjectConfig
) -> pd.DataFrame:
    """Clean the curated rental dataset before splitting.

    This includes handling any remaining missing values and ensuring the
    datetime column is in the correct format.
    """
    data = csv_io.read(project_config.curated_path)
    num_rows_before = len(data)
    cols_before = data.columns.tolist()
    data = _data_clean_helper(data)
    cols_after = data.columns.tolist()
    num_rows_after = len(data)
    context.add_output_metadata(
        metadata={
            "num_rows_before": num_rows_before,
            "num_rows_after": num_rows_after,
            "rows_removed": num_rows_before - num_rows_after,
            "cols_before": cols_before,
            "cols_after": cols_after,
            "cols_removed": list(set(cols_before) - set(cols_after)),
        }
        | metadata_extractor(data)
    )
    return data


@dg.multi_asset(
    outs={
        "train_data": dg.AssetOut(),
        "test_data": dg.AssetOut(),
    },
    deps=["clean_curated_data"],
    group_name="data_preparation",
)
def train_test_split(
    context, clean_curated_data: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split the curated rental dataset into train and test sets.

    It uses a simple time-based split, where the last 20% of the data is used
    as the test set.
    """
    try:
        data = clean_curated_data.sort_values("datetime")
        split_index = int(len(data) * 0.8)
        train_data = data.iloc[:split_index]
        test_data = data.iloc[split_index:]
        context.add_output_metadata(
            output_name="train_data",
            metadata=metadata_extractor(train_data),
        )
        context.add_output_metadata(
            output_name="test_data",
            metadata=metadata_extractor(test_data),
        )
        return (train_data, test_data)
    except Exception as e:
        raise Exception(
            f"error occurred while splitting train and test data: {e}"
        )
