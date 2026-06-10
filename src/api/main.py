"""Main API for the bike rental project."""

from fastapi import FastAPI, HTTPException

from bike_rental.defs.resources.project_config import ProjectConfig
from models.model_registry import get_champion, load_champion_model

FeatureInput = ProjectConfig.get_feature_model()

app = FastAPI(title="Bike Rental Prediction API")


@app.get("/current-champion")
def read_champion_metadata():
    """Endpoint to get current champion model metadata."""
    champion_data = get_champion()
    if champion_data is None:
        return {"error": "No model has been promoted to champion yet."}
    return {
        "status": "active",
        "champion_version": champion_data["version"],
        "metrics": champion_data["metrics"],
        "model_type": champion_data["model_type"],
    }


@app.get("/")
def index():
    """Root endpoint."""
    return {
        "message": "Welcome to the Bike Rental API!\
          Visit /current-champion to see the current champion model metadata."
    }


@app.post("/predict")
def predict(features: dict):
    """Bike Rental Prediction endpoint."""
    try:
        model = load_champion_model()
        features = FeatureInput(**features)
        df = ProjectConfig.features_to_dataframe(features)
        prediction = model.predict(df)

        return {"prediction": float(prediction[0]), "status": "success"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
