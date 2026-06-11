"""Assets for model registry."""

from typing import Any

import dagster as dg

from lakefs_mod.lsf_config import LFSConfig
from models.mlflow_utils import mlflow_manager

lfs_conf = LFSConfig()


@dg.asset(
    group_name="model_registry",
    ins={"best_model": dg.AssetIn("train_and_score_all_models")},
)
def register_candidate_model(
    context: dg.AssetExecutionContext, best_model: dict[str, Any]
) -> dict[str, Any]:
    """Register best model as candidate in MLflow."""
    description = (
        f"### Bike Rental Demand Forecaster\n\n"
        f"* **Model Type:** `{best_model['model_type']}`\n"
        f"* **R² Score:** `{best_model['r2']:.4f}`\n"
        f"* **Dagster Run ID:** `{str(context.run.run_id)}`\n\n"
        f"**Lineage Tracking:**\n"
        f"Trained automatically via Dagster pipeline.\
            Check the MLflow Run tags to find the exact\
            LakeFS data branch and commit ID used for this version."
    )

    registry_info = mlflow_manager.register_candidate(
        run_id=best_model["run_id"],
        artifact_path=best_model["artifact_path"],
        description=description,
    )

    result = {
        "run_id": best_model["run_id"],
        "model_type": best_model["model_type"],
        "r2": best_model["r2"],
        "model_version": registry_info["model_version"],
        "model_uri": registry_info["model_uri"],
    }
    context.add_output_metadata(result)
    return result


@dg.asset(
    group_name="model_registry",
    ins={"candidate": dg.AssetIn("register_candidate_model")},
)
def promote_champion_model(
    context: dg.AssetExecutionContext, candidate: dict[str, Any]
) -> dict[str, Any]:
    """Promotes model if better than current champion."""
    ret = mlflow_manager.evaluate_and_promote_champion(candidate)
    context.add_output_metadata(ret)
    return ret


@dg.asset(group_name="pipeline_management", deps=["promote_champion_model"])
def merge_data_to_main(context: dg.AssetExecutionContext):
    """Merge the successful Dagster run data back to the main LakeFS branch."""
    run_id = str(context.run.run_id).split("-")[0]
    branch_name = lfs_conf.get_asset_branch("curated_rental_dataset", run_id)

    try:
        merge_result = lfs_conf.merge_to_main(branch_name)
        context.add_output_metadata({"merge_result": merge_result})
        context.log.info(
            f"Successfully merged {branch_name} to main: {merge_result}"
        )
    except Exception as e:
        context.log.warning(f"Merge failed or no changes to merge: {e}")
