"""Main API for the bike rental project."""

from fastapi import FastAPI

from models.model_registry import get_champion

app = FastAPI()


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


# @app.post("/predict")
# def predict():
#     """Prediction endpoint."""
#     try:
#         model = load_champion_model()
#         model.predict()
#         return {"message": "Prediction endpoint not yet implemented."}
#     except ValueError as e:
#         return {"error": str(e)}
