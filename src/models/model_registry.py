"""Model registry assets."""

from typing import Any

from .mlflow_utils import init_mlflow

mlflow_config = init_mlflow()


def get_champion() -> dict[str, Any] | None:
    """Return champion model version and r2 metric."""
    try:
        champ = mlflow_config["client"].get_model_version_by_alias(
            name=mlflow_config["MODEL_NAME"],
            alias=mlflow_config["CHAMPION_ALIAS"],
        )
        run = mlflow_config["client"].get_run(champ.run_id)
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


def is_first_champion(
    champion: dict[str, Any] | None,
    candidate: dict[str, Any],
    candidate_r2: float,
) -> dict[str, Any]:
    """If no champion exists, promote candidate to champion."""
    if champion is None:
        mlflow_config["client"].set_registered_model_alias(
            name=mlflow_config["MODEL_NAME"],
            version=candidate["model_version"],
            alias=mlflow_config["CHAMPION_ALIAS"],
        )
        return {
            "promoted": True,
            "reason": "first champion",
            "champion_version": candidate["model_version"],
            "champion_r2": candidate_r2,
        }
    return {}


def evaluate_and_update_champion(candidate: dict[str, Any]) -> dict[str, Any]:
    """Determine if candidate can be promoted to champion."""
    client = mlflow_config["client"]
    model_name = mlflow_config["MODEL_NAME"]
    champion_alias = mlflow_config["CHAMPION_ALIAS"]
    candidate_r2 = float(candidate["r2"])
    champion = get_champion()
    ret = is_first_champion(champion, candidate, candidate_r2)
    if ret:
        return ret

    champion_version, champion_r2 = champion["version"], champion["r2"]
    if candidate_r2 > champion_r2:
        client.set_registered_model_alias(
            name=model_name,
            version=candidate["model_version"],
            alias=champion_alias,
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


def load_champion_model():
    """Load champion model from MLflow."""
    champion = get_champion()
    if champion is None:
        raise ValueError("No champion model found.")
    version, _ = champion["version"], champion["r2"]
    model_uri = f"models:/{mlflow_config['MODEL_NAME']}/{version}"
    return mlflow_config["mlflow"].pyfunc.load_model(model_uri)
