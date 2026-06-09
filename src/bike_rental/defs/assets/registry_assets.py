"""Assets for model registry."""

from typing import Any

import dagster as dg
import mlflow

from models.mlflow_utils import init_mlflow
from models.model_registry import evaluate_and_update_champion

mlflow_config = init_mlflow()


@dg.asset(
    group_name="model_registry",
    ins={"best_model": dg.AssetIn("train_and_score_all_models")},
)
def register_candidate_model(
    context: dg.AssetExecutionContext, best_model: dict[str, Any]
) -> dict[str, Any]:
    """Register best model as candidate in MLflow."""
    model_uri = f"runs:/{best_model['run_id']}/{best_model['artifact_path']}"

    registered = mlflow.register_model(
        model_uri=model_uri,
        name=mlflow_config["MODEL_NAME"],
    )

    mlflow_config["client"].set_registered_model_alias(
        name=mlflow_config["MODEL_NAME"],
        version=registered.version,
        alias=mlflow_config["CANDIDATE_ALIAS"],
    )

    result = {
        "run_id": best_model["run_id"],
        "model_type": best_model["model_type"],
        "r2": best_model["r2"],
        "model_version": registered.version,
        "model_uri": model_uri,
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
    ret = evaluate_and_update_champion(candidate)
    context.add_output_metadata(ret)
    return ret
