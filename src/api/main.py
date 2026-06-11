"""Main API for the bike rental project."""

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

from bike_rental.defs.resources.project_config import ProjectConfig
from models.model_registry import get_champion, load_champion_model

FeatureInput = ProjectConfig.get_feature_model()

ml_models = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager to load champion model at startup."""
    print("Loading champion model from MLflow...")
    ml_models["champion"] = load_champion_model()
    yield
    ml_models.clear()


app = FastAPI(title="Bike Rental Prediction API", lifespan=lifespan)


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
          Visit /docs to Try out the different endpoints."
    }


@app.post("/predict")
def predict(features: dict):
    """Bike Rental Prediction endpoint."""
    try:
        model = ml_models["champion"]

        features_obj = FeatureInput(**features)
        df = ProjectConfig.features_to_dataframe(features_obj)
        prediction = model.predict(df)

        return {"prediction": float(prediction[0]), "status": "success"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
