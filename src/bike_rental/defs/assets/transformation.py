"""Assets for the final rental feature engineering stage."""

import dagster as dg
import pandas as pd

from bike_rental.defs.assets.helper import (
    add_time_based_features,
    metadata_extractor,
)
from bike_rental.defs.resources import CSVIO, ProjectConfig
from lakefs_mod import lfs_conf
from models import mlflow_manager


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
        asset_name = "curated_rental_dataset"
        run_id = str(context.run.run_id).split("-")[0]
        new_branch = lfs_conf.get_asset_branch(asset_name, run_id)
        lfs_conf.create_branch(new_branch)

        data = rentals_with_holidays.copy()
        data["is_holiday"] = data["holiday"].notna().astype(int)
        data.drop(columns=["holiday"], inplace=True)
        data.drop(columns=["date"], inplace=True)

        lfs_conf.write_csv(data, project_config.curated_path, new_branch)

        uncommitted_changes = lfs_conf.get_uncommitted(new_branch)

        with mlflow_manager.start_run(run_name=asset_name) as active_run:
            commit_metadata = {
                "dagster_run_id": str(context.run.run_id),
                "mlflow_run_id": active_run.info.run_id,
                "asset_name": asset_name,
            }
            commit_ref = lfs_conf.commit_if_changed(
                branch=new_branch,
                message=f"Dagster update: {asset_name} pipeline",
                metadata=commit_metadata,
            )
            commit_id = commit_ref.id if commit_ref else None
            mlflow_manager.log_lakefs_metadata(
                asset_name=asset_name,
                branch=new_branch,
                commit_id=commit_id,
                diff_summary=uncommitted_changes,
            )
        diff_summary = (
            f"{len(uncommitted_changes)} object(s) changed"
            if uncommitted_changes
            else "No changes"
        )
        dagster_metadata = metadata_extractor(data) | {
            "lakefs_branch": new_branch,
            "lakefs_commit_id": commit_id or "Skipped (No changes)",
            "diff_summary": diff_summary,
        }
        context.add_output_metadata(metadata=dagster_metadata)

        return None
    except Exception as e:
        raise RuntimeError(f"Error in writing curated rental dataset: {e}")
