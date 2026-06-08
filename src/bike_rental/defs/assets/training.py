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

experiment_name = "MLflow Integration with Dagster"

try:
    mlflow.set_experiment(experiment_name)
except Exception as e:
    print(f"Error setting MLflow experiment: {e}")
    mlflow.create_experiment(experiment_name)
    mlflow.set_experiment(experiment_name)


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
    best_run_id = None
    best_model_path = None
    best_r2 = float("-inf")

    for model_type in ModelType:
        parameters = project_config.params.get(model_type.value, {})
        model = ModelFactory.create(model_name=model_type, params=parameters)
        trainer = Trainer(model)
        artifact_path = f"{model_type.value}_model"
        with mlflow.start_run(run_name=f"train_{model_type.value}") as run:
            trainer.fit(X_train, y_train)
            metrics = trainer.evaluate(X_test, y_test)

            parameters.update({"model_name": str(model_type.value)})

            mlflow.log_params(parameters)
            mlflow.log_metrics(metrics)
            if model_type == ModelType.XGBOOST and hasattr(mlflow, "xgboost"):
                mlflow.xgboost.log_model(model, artifact_path)
            else:
                mlflow.sklearn.log_model(model, artifact_path)

            if metrics.get("r2", float("-inf")) > best_r2:
                best_r2 = metrics["r2"]
                best_run_id = run.info.run_id
                best_model_path = artifact_path

        results[model_type.value] = metrics

    model_uri = f"runs:/{best_run_id}/{best_model_path}"

    registered_model = mlflow.register_model(
        model_uri=model_uri, name="rental_forecast_model"
    )
    context.add_output_metadata(
        {
            "Evaluation Metrics": results,
            "best_model_run_id": best_run_id,
            "best_model_r2": best_r2,
            "registered_model_version": registered_model.version,
            "model_uri": model_uri,
            "model_name": registered_model.name,
        }
    )
    return results
