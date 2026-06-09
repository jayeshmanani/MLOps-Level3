"""Asset definitions for training and evaluating models."""

import os
from typing import Any

import dagster as dg
import load_dotenv
import mlflow
import mlflow.sklearn
import pandas as pd
from mlflow import MlflowClient

from bike_rental.defs.assets.model.factory import ModelFactory, ModelType
from bike_rental.defs.assets.model.trainer import Trainer
from bike_rental.defs.resources.project_config import ProjectConfig

load_dotenv.load_dotenv()

try:
    import mlflow.xgboost
except Exception:
    mlflow_xgboost = None

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
MODEL_NAME = os.getenv("MODEL_NAME", "rental_forecast_model")
CANDIDATE_ALIAS = os.getenv("CANDIDATE_ALIAS", "candidate")
CHAMPION_ALIAS = os.getenv("CHAMPION_ALIAS", "champion")

EXPERIMENT_NAME = os.getenv(
    "EXPERIMENT_NAME", "MLflow Integration with Dagster"
)

client = MlflowClient(MLFLOW_TRACKING_URI)

mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

try:
    mlflow.set_experiment(EXPERIMENT_NAME)
except Exception:
    mlflow.create_experiment(EXPERIMENT_NAME)
    mlflow.set_experiment(EXPERIMENT_NAME)


def _log_model(
    model_type: ModelType,
    model: Any,
    parameters: dict[str, object],
    metrics: dict[str, float],
) -> None:
    try:
        artifact_path = parameters.get(
            "model_name", f"{model_type.value}_model"
        )

        mlflow.log_params(parameters)
        mlflow.log_metrics(metrics)

        if model_type == ModelType.XGBOOST and hasattr(mlflow, "xgboost"):
            mlflow.xgboost.log_model(model, artifact_path)
        else:
            mlflow.sklearn.log_model(model, artifact_path)

    except Exception as e:
        print(f"MLflow logging error: {e}")


@dg.asset(group_name="model_training_evaluation")
def train_and_score_all_models(
    context: dg.AssetExecutionContext,
    train_df: pd.DataFrame,
    test_df: pd.DataFrame,
    project_config: ProjectConfig,
) -> dict[str, Any]:
    """Trains all models and returns ONLY the best candidate metadata."""
    X_train = train_df[project_config.FEATURES]
    y_train = train_df[project_config.TARGET]

    X_test = test_df[project_config.FEATURES]
    y_test = test_df[project_config.TARGET]

    best = {
        "run_id": None,
        "artifact_path": None,
        "r2": float("-inf"),
        "model_type": None,
    }

    for model_type in ModelType:
        params = dict(project_config.params.get(model_type.value, {}))
        model = ModelFactory.create(model_name=model_type, params=params)
        trainer = Trainer(model)

        with mlflow.start_run(run_name=f"train_{model_type.value}") as run:
            artifact_path = model.__class__.__name__
            params["model_name"] = artifact_path

            trainer.fit(X_train, y_train)
            metrics = trainer.evaluate(X_test, y_test)

            _log_model(model_type, model, params, metrics)

            r2 = float(metrics.get("r2", float("-inf")))

            if r2 > best["r2"]:
                best = {
                    "run_id": run.info.run_id,
                    "artifact_path": artifact_path,
                    "r2": r2,
                    "model_type": model_type.value,
                }

    context.add_output_metadata(best)
    return best


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
        name=MODEL_NAME,
    )

    client.set_registered_model_alias(
        name=MODEL_NAME,
        version=registered.version,
        alias=CANDIDATE_ALIAS,
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


def _get_champion() -> tuple[str, float] | None:
    """Return champion model version and r2 metric."""
    try:
        champ = client.get_model_version_by_alias(
            name=MODEL_NAME,
            alias=CHAMPION_ALIAS,
        )
        run = client.get_run(champ.run_id)
        r2 = float(run.data.metrics.get("r2", float("-inf")))
        return champ.version, r2
    except Exception:
        return None


def _is_first_champion(
    champion: tuple[str, float] | None,
    candidate: dict[str, Any],
    candidate_r2: float,
) -> dict[str, Any]:
    """If no champion exists, promote candidate to champion."""
    if champion is None:
        client.set_registered_model_alias(
            name=MODEL_NAME,
            version=candidate["model_version"],
            alias=CHAMPION_ALIAS,
        )
        return {
            "promoted": True,
            "reason": "first champion",
            "champion_version": candidate["model_version"],
            "champion_r2": candidate_r2,
        }
    return {}


def _can_promote_to_champion(
    candidate: dict[str, Any], champion: tuple[str, float], candidate_r2: float
) -> dict[str, Any]:
    """Determine if candidate can be promoted to champion."""
    champion_version, champion_r2 = champion
    if candidate_r2 > champion_r2:
        client.set_registered_model_alias(
            name=MODEL_NAME,
            version=candidate["model_version"],
            alias=CHAMPION_ALIAS,
        )

        return {
            "promoted": True,
            "reason": f"beaten {champion_version}",
            "champion_version": candidate["model_version"],
            "champion_r2": candidate_r2,
        }
    return {
        "promoted": False,
        "reason": f"kept {champion_version}",
        "champion_version": champion_version,
        "champion_r2": champion_r2,
        "candidate_r2": candidate_r2,
    }


@dg.asset(
    group_name="model_registry",
    ins={"candidate": dg.AssetIn("register_candidate_model")},
)
def promote_champion_model(
    context: dg.AssetExecutionContext, candidate: dict[str, Any]
) -> dict[str, Any]:
    """Promotes model if better than current champion."""
    candidate_r2 = float(candidate["r2"])

    champion = _get_champion()
    ret = _is_first_champion(champion, candidate, candidate_r2)
    if ret:
        context.add_output_metadata(ret)
        return ret
    ret = _can_promote_to_champion(candidate, champion, candidate_r2)
    context.add_output_metadata(ret)
    return ret
