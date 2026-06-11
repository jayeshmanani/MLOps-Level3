"""MLflow utility functions."""

import os

import load_dotenv
import mlflow
from mlflow import MlflowClient

load_dotenv.load_dotenv()


def init_mlflow() -> dict[str, object]:
    """Set up MLflow configs. Call this at the top of every file."""
    MLFLOW_TRACKING_URI = os.getenv(
        "MLFLOW_TRACKING_URI", "http://localhost:5000"
    )
    MODEL_NAME = os.getenv("MODEL_NAME", "rental_forecast_model")
    CANDIDATE_ALIAS = os.getenv("CANDIDATE_ALIAS", "candidate")
    CHAMPION_ALIAS = os.getenv("CHAMPION_ALIAS", "champion")
    EXPERIMENT_NAME = os.getenv(
        "EXPERIMENT_NAME", "MLflow Integration with Dagster"
    )

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

    try:
        mlflow.set_experiment(EXPERIMENT_NAME)
    except Exception:
        mlflow.create_experiment(EXPERIMENT_NAME)
        mlflow.set_experiment(EXPERIMENT_NAME)

    return {
        "client": MlflowClient(MLFLOW_TRACKING_URI),
        "MLFLOW_TRACKING_URI": MLFLOW_TRACKING_URI,
        "MODEL_NAME": MODEL_NAME,
        "CANDIDATE_ALIAS": CANDIDATE_ALIAS,
        "CHAMPION_ALIAS": CHAMPION_ALIAS,
        "EXPERIMENT_NAME": EXPERIMENT_NAME,
    }


def log_lakefs_metadata(*, asset_name, branch, commit_id, diff_summary):
    """Log lakeFS metadata to MLflow."""
    mlflow.log_param("asset_name", asset_name)
    mlflow.log_param("lakefs_branch", branch)
    mlflow.log_param("lakefs_commit", commit_id)

    mlflow.log_metric("num_changes", len(diff_summary or []))
