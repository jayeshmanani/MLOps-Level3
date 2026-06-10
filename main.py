"""main module for the mlops-level3 project.

It serves as the entry point for the application.
"""

import multiprocessing
import os

import uvicorn


def run_mlflow():
    """Start the MLflow tracking server on http://127.0.0.1:5000."""
    print("📦 Starting MLflow UI...")
    os.system(
        "mlflow server --host 127.0.0.1 --port 5000 --backend-store-uri sqlite:///mlflow.db"
    )


def run_dagster():
    """Start the Dagster Web UI on http://127.0.0.1:3000."""
    print("🎛️  Starting Dagster Web UI...")
    os.environ["DAGSTER_HOME"] = os.getcwd()
    os.system("dagster dev -f src/bike_rental/definitions.py")


def run_api():
    """Start the FastAPI server on http://127.0.0.1:8001."""
    print("⚡ Starting FastAPI Endpoint...")
    uvicorn.run("src.api.main:app", host="127.0.0.1", port=8001, reload=True)


if __name__ == "__main__":
    os.environ["PYTHONPATH"] = os.getcwd()

    p1 = multiprocessing.Process(target=run_mlflow)
    p2 = multiprocessing.Process(target=run_dagster)
    p3 = multiprocessing.Process(target=run_api)

    p1.start()
    p2.start()
    p3.start()

    try:
        p1.join()
        p2.join()
        p3.join()
    except KeyboardInterrupt:
        print("\n🛑 Closing all apps safely...")
        p1.terminate()
        p2.terminate()
        p3.terminate()
