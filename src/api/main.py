"""Main API for the bike rental project."""

from contextlib import asynccontextmanager

import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from bike_rental.defs.assets.helper import add_time_based_features
from bike_rental.defs.resources.project_config import ProjectConfig
from models.mlflow_utils import mlflow_manager

ml_models = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager to load the champion model at startup."""
    print("Loading champion model from MLflow...")
    try:
        ml_models["champion"] = mlflow_manager.load_champion_model()
        print("Model loaded successfully.")
    except Exception as e:
        print(f"Warning: Could not load model at startup: {e}")
    yield
    ml_models.clear()


app = FastAPI(title="Bike Rental Prediction API", lifespan=lifespan)


class RentalPredictionRequest(BaseModel):
    """Request model for bike rental prediction."""

    datetime: str = Field(
        ...,
        description="ISO format datetime string",
        examples=["2026-06-11T14:00:00"],
    )
    is_holiday: int = Field(
        default=0, description="1 if it is a holiday, 0 otherwise", examples=[0]
    )


@app.get("/current-champion")
def read_champion_metadata():
    """Endpoint to get current champion model metadata."""
    champion_data = mlflow_manager.get_champion()
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
          Visit /docs to Try out the different endpoints."
    }


@app.post("/predict")
def predict(request: RentalPredictionRequest):
    """Bike Rental Prediction endpoint."""
    try:
        if "champion" not in ml_models:
            raise RuntimeError(
                "Model is not loaded. Ensure a champion model exists in MLflow."
            )

        model = ml_models["champion"]
        input_df = pd.DataFrame(
            [{"datetime": request.datetime, "is_holiday": request.is_holiday}]
        )
        engineered_df = add_time_based_features(input_df, col="datetime")
        features_to_pass = engineered_df[ProjectConfig.FEATURES]
        prediction = model.predict(features_to_pass)

        return {"prediction": float(prediction[0]), "status": "success"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
