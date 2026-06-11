"""MLflow utility functions."""

import os

import load_dotenv
import mlflow
from mlflow import MlflowClient

load_dotenv.load_dotenv()


class MLflowManager:
    """Singleton manager for MLflow operations and registry interactions."""

    _instance = None

    def __new__(cls):
        """Ensure only one instance of MLflowManager exists."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init()
        return cls._instance

    def _init(self):
        self.tracking_uri = os.getenv(
            "MLFLOW_TRACKING_URI", "http://localhost:5000"
        )
        self.model_name = os.getenv("MODEL_NAME", "rental_forecast_model")
        self.candidate_alias = os.getenv("CANDIDATE_ALIAS", "candidate")
        self.champion_alias = os.getenv("CHAMPION_ALIAS", "champion")
        self.experiment_name = os.getenv(
            "EXPERIMENT_NAME", "MLflow Integration with Dagster"
        )

        mlflow.set_tracking_uri(self.tracking_uri)
        self.client = MlflowClient(self.tracking_uri)

        try:
            mlflow.set_experiment(self.experiment_name)
        except Exception:
            mlflow.create_experiment(self.experiment_name)
            mlflow.set_experiment(self.experiment_name)

    def start_run(self, run_name: str):
        """Wrap for mlflow.start_run to keep Dagster decoupled."""
        return mlflow.start_run(run_name=run_name)

    def log_lakefs_metadata(
        self, asset_name: str, branch: str, commit_id: str, diff_summary: list
    ) -> None:
        """Log lakeFS data pipeline metadata to the active MLflow run."""
        try:
            mlflow.log_param("asset_name", asset_name)
            mlflow.log_param("lakefs_branch", branch)
            mlflow.log_param("lakefs_commit", commit_id)
            mlflow.log_metric("num_changes", len(diff_summary or []))
        except Exception as e:
            print(f"Failed to log LakeFS metadata to MLflow: {e}")

    def register_candidate(
        self, run_id: str, artifact_path: str, description: str = None
    ) -> dict:
        """Register a run as a model, sets the.

        candidate alias, and updates the description.
        """
        model_uri = f"runs:/{run_id}/{artifact_path}"

        registered = mlflow.register_model(
            model_uri=model_uri,
            name=self.model_name,
        )

        self.client.set_registered_model_alias(
            name=self.model_name,
            version=registered.version,
            alias=self.candidate_alias,
        )

        if description:
            self.client.update_model_version(
                name=self.model_name,
                version=registered.version,
                description=description,
            )

        return {"model_version": registered.version, "model_uri": model_uri}

    def log_model_artifacts(
        self,
        model_type: str,
        model: object,
        parameters: dict,
        metrics: dict,
        feature_names: list,
        data_branch: str,
        commit_id: str = None,
    ) -> None:
        """Log parameters, metrics, lakefs lineage, and.

        the model artifact to the active run.
        """
        try:
            artifact_path = parameters.get("model_name", f"{model_type}_model")

            mlflow.log_params(parameters)
            mlflow.log_metrics(metrics)
            mlflow.log_param("feature_names", ",".join(feature_names))

            mlflow.log_param("lakefs_training_data_branch", data_branch)
            if commit_id:
                mlflow.log_param("lakefs_training_data_commit", commit_id)

            # Route to the correct model flavor
            if model_type == "xgboost" and hasattr(mlflow, "xgboost"):
                mlflow.xgboost.log_model(model, artifact_path)
            else:
                mlflow.sklearn.log_model(model, artifact_path)

        except Exception as e:
            print(f"MLflow logging error: {e}")

    def get_champion(self) -> dict | None:
        """Return champion model version and r2 metric."""
        try:
            champ = self.client.get_model_version_by_alias(
                name=self.model_name,
                alias=self.champion_alias,
            )
            run = self.client.get_run(champ.run_id)
            metrics = run.data.metrics
            return {
                "version": champ.version,
                "metrics": metrics,
                "r2": metrics.get("r2", float("-inf")),
                "run_id": champ.run_id,
                "model_type": run.data.params.get("model_name", "unknown"),
            }
        except Exception:
            return None

    def evaluate_and_promote_champion(self, candidate: dict) -> dict:
        """Determine if a candidate can be promoted to champion."""
        candidate_r2 = float(candidate["r2"])
        champion = self.get_champion()

        # 1. No champion exists yet? Promote immediately.
        if champion is None:
            self.client.set_registered_model_alias(
                name=self.model_name,
                version=candidate["model_version"],
                alias=self.champion_alias,
            )
            return {
                "promoted": True,
                "reason": "first champion",
                "champion_version": candidate["model_version"],
                "champion_r2": candidate_r2,
            }

        # 2. Compare against existing champion
        champion_version = champion["version"]
        champion_r2 = champion["r2"]

        if candidate_r2 > champion_r2:
            self.client.set_registered_model_alias(
                name=self.model_name,
                version=candidate["model_version"],
                alias=self.champion_alias,
            )
            return {
                "promoted": True,
                "reason": f"beaten {champion_version}",
                "champion_version": candidate["model_version"],
                "champion_r2": candidate_r2,
            }

        # 3. Candidate was not better, keep the current champion
        return {
            "promoted": False,
            "reason": f"kept {champion_version}",
            "champion_version": champion_version,
            "champion_r2": champion_r2,
            "candidate_r2": candidate_r2,
        }

    def load_champion_model(self):
        """Load champion model from MLflow into memory."""
        champion = self.get_champion()
        if champion is None:
            raise ValueError("No champion model found in the registry.")

        version = champion["version"]
        model_uri = f"models:/{self.model_name}/{version}"
        return mlflow.pyfunc.load_model(model_uri)


mlflow_manager = MLflowManager()
