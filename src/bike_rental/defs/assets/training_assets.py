"""Asset definitions for training and evaluating models."""

from typing import Any

import dagster as dg
import mlflow
import mlflow.sklearn
import pandas as pd

from bike_rental.defs.resources.project_config import ProjectConfig
from lakefs_mod.lsf_config import LFSConfig
from models.factory import ModelFactory, ModelType
from models.mlflow_utils import init_mlflow
from models.trainer import Trainer

lfs_conf = LFSConfig()
mlflow_config = init_mlflow()

try:
    import mlflow.xgboost
except Exception:
    mlflow_xgboost = None


def _log_model(
    model_type: ModelType,
    model: Any,
    parameters: dict[str, object],
    metrics: dict[str, float],
    feature_names: list[str],
) -> None:
    try:
        artifact_path = parameters.get(
            "model_name", f"{model_type.value}_model"
        )

        mlflow.log_params(parameters)
        mlflow.log_metrics(metrics)
        mlflow.log_param("feature_names", ",".join(feature_names))

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
    run_id = str(context.run.run_id).split("-")[0]
    data_branch = lfs_conf.get_asset_branch("curated_rental_dataset", run_id)

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

            _log_model(model_type, model, params, metrics, X_train.columns)
            mlflow.log_param("lakefs_training_data_branch", data_branch)

            try:
                commit_id = lfs_conf.repo.branch(data_branch).get_commit().id
                mlflow.log_param("lakefs_training_data_commit", commit_id)
            except Exception:
                pass

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
