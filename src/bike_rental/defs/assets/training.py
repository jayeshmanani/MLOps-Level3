"""Asset definitions for training and evaluating models."""

import os

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

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI")
client = MlflowClient(MLFLOW_TRACKING_URI)

mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
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


def _helper_mlflow_log_model(
    model_type: ModelType,
    model: any,
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
        print(f"Error logging model to MLflow: {e}")


def _helper_register_best_model() -> dict[str, object]:
    try:
        last_3_runs = client.search_runs(
            experiment_ids=[
                client.get_experiment_by_name(experiment_name).experiment_id
            ],
            max_results=3,
            order_by=["metrics.r2 DESC"],
        )
        best_run = last_3_runs[0]
        best_run_id = best_run.info.run_id
        best_model_path = best_run.data.params.get("model_name")
        model_uri = f"runs:/{best_run_id}/{best_model_path}"
        registered_model = mlflow.register_model(
            model_uri=model_uri, name="rental_forecast_model"
        )
        return {
            "best_model_run_id": best_run_id,
            "best_model_metrics": best_run.data.metrics,
            "registered_model_version": registered_model.version,
            "model_uri": model_uri,
            "model_name": registered_model.name,
        }
    except Exception as e:
        print(f"Error registering best model to MLflow: {e}")
    return {}


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
        parameters = project_config.params.get(model_type.value, {})
        model = ModelFactory.create(model_name=model_type, params=parameters)
        trainer = Trainer(model)
        with mlflow.start_run(run_name=f"train_{model_type.value}"):
            parameters.update({"model_name": f"{model.__class__.__name__}"})

            trainer.fit(X_train, y_train)
            metrics = trainer.evaluate(X_test, y_test)

            _helper_mlflow_log_model(model_type, model, parameters, metrics)

        results[model_type.value] = metrics
    registered_model_info = _helper_register_best_model()

    context.add_output_metadata(
        {
            "Evaluation Metrics": results,
            **registered_model_info,
        }
    )
    return results
