"""Asset definitions for training and evaluating models."""

import dagster as dg
import mlflow
import mlflow.sklearn
import pandas as pd

from bike_rental.defs.assets.model.factory import ModelFactory, ModelType
from bike_rental.defs.assets.model.trainer import Trainer
from bike_rental.defs.resources.project_config import ProjectConfig

try:
    import mlflow.xgboost
except Exception:
    mlflow_xgboost = None

mlflow.set_experiment("MLflow Integration with Dagster")


class TrainingConfig(dg.Config):
    """Configuration for selecting a single model and its parameters."""

    model_name: ModelType
    params: dict[str, object] = {}


@dg.asset(group_name="model_training_evaluation")
def train_and_score_all_models(
    context: dg.AssetExecutionContext,
    train_df: pd.DataFrame,
    test_df: pd.DataFrame,
    project_config: ProjectConfig,
) -> dict[str, dict[str, float]]:
    """Train and evaluate all models defined in `ModelType`.

    Logs metrics and model artifact to MLflow for each model and emits
    a Dagster AssetMaterialization containing the metrics.

    Returns a dict mapping model_name -> metrics.
    """
    X_train = train_df[project_config.FEATURES]
    y_train = train_df[project_config.TARGET]

    X_test = test_df[project_config.FEATURES]
    y_test = test_df[project_config.TARGET]

    results: dict[str, dict[str, float]] = {}

    for model_type in ModelType:
        model_name = model_type
        parameters = project_config.params.get(model_type.value, {})
        model = ModelFactory.create(model_name=model_name, params=parameters)
        trainer = Trainer(model)

        trainer.fit(X_train, y_train)

        metrics = trainer.evaluate(X_test, y_test)

        # Log to MLflow
        with mlflow.start_run(run_name=f"train_{model_type.value}"):
            mlflow.log_params(parameters)
            mlflow.log_metrics(metrics)
            try:
                if model_type == ModelType.XGBOOST and hasattr(
                    mlflow, "xgboost"
                ):
                    mlflow.xgboost.log_model(model, artifact_path="model")
                else:
                    mlflow.sklearn.log_model(model, artifact_path="model")
            except Exception as e:
                context.log.warn(
                    f"Failed to log model to MLflow for {model_type}: {e}"
                )

        # Keep a record in the logs for each model
        context.log.info(f"Trained {model_type.value} -> metrics={metrics}")

        results[model_type.value] = metrics

    context.add_output_metadata({"models": results})
    return results
