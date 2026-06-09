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


# def _get_champion() -> tuple[str, float] | None:
#     """Return champion model version and r2 metric."""
#     try:
#         champ = mlflow_config["client"].get_model_version_by_alias(
#             name=mlflow_config["MODEL_NAME"],
#             alias=mlflow_config["CHAMPION_ALIAS"],
#         )
#         run = mlflow_config["client"].get_run(champ.run_id)
#         r2 = float(run.data.metrics.get("r2", float("-inf")))
#         return champ.version, r2
#     except Exception:
#         return None


# def _is_first_champion(
#     champion: tuple[str, float] | None,
#     candidate: dict[str, Any],
#     candidate_r2: float,
# ) -> dict[str, Any]:
#     """If no champion exists, promote candidate to champion."""
#     if champion is None:
#         mlflow_config["client"].set_registered_model_alias(
#             name=mlflow_config["MODEL_NAME"],
#             version=candidate["model_version"],
#             alias=mlflow_config["CHAMPION_ALIAS"],
#         )
#         return {
#             "promoted": True,
#             "reason": "first champion",
#             "champion_version": candidate["model_version"],
#             "champion_r2": candidate_r2,
#         }
#     return {}


# def _can_promote_to_champion(
#     candidate: dict[str, Any], champion: tuple[str, float], candidate_r2: float
# ) -> dict[str, Any]:
#     """Determine if candidate can be promoted to champion."""
#     champion_version, champion_r2 = champion
#     if candidate_r2 > champion_r2:
#         mlflow_config["client"].set_registered_model_alias(
#             name=mlflow_config["MODEL_NAME"],
#             version=candidate["model_version"],
#             alias=mlflow_config["CHAMPION_ALIAS"],
#         )

#         return {
#             "promoted": True,
#             "reason": f"beaten {champion_version}",
#             "champion_version": candidate["model_version"],
#             "champion_r2": candidate_r2,
#         }
#     return {
#         "promoted": False,
#         "reason": f"kept {champion_version}",
#         "champion_version": champion_version,
#         "champion_r2": champion_r2,
#         "candidate_r2": candidate_r2,
#     }


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
