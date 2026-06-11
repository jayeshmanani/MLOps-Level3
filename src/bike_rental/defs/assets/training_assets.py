"""Asset definitions for training and evaluating models."""

from typing import Any

import dagster as dg
import pandas as pd

from bike_rental.defs.resources.project_config import ProjectConfig
from lakefs_mod import lfs_conf
from models import ModelFactory, ModelType, Trainer, mlflow_manager


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
    try:
        commit_id = lfs_conf.repo.branch(data_branch).get_commit().id
    except Exception:
        commit_id = None

    X_train, y_train = (
        train_df[project_config.FEATURES],
        train_df[project_config.TARGET],
    )
    X_test, y_test = (
        test_df[project_config.FEATURES],
        test_df[project_config.TARGET],
    )

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

        with mlflow_manager.start_run(
            run_name=f"train_{model_type.value}"
        ) as run:
            artifact_path = model.__class__.__name__
            params["model_name"] = artifact_path

            trainer.fit(X_train, y_train)
            metrics = trainer.evaluate(X_test, y_test)

            mlflow_manager.log_model_artifacts(
                model_type=model_type.value,
                model=model,
                parameters=params,
                metrics=metrics,
                feature_names=list(X_train.columns),
                data_branch=data_branch,
                commit_id=commit_id,
            )

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
