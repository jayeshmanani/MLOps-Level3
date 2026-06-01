"""Evaluation assets for the bike rental prediction model."""

import dagster as dg
import numpy as np
from sklearn.metrics import mean_absolute_error, mean_squared_error


@dg.asset(group_name="evaluations")
def linear_regression_metrics(context, linear_regression_model, X_test, y_test):
    """Evaluate the Linear Regression model on the test data."""
    preds = linear_regression_model.predict(X_test)

    mae = mean_absolute_error(y_test, preds)
    rmse = np.sqrt(mean_squared_error(y_test, preds))

    context.add_output_metadata(
        {"mae": float(mae), "rmse": float(rmse), "model": "LinearRegression"}
    )

    return {"mae": mae, "rmse": rmse}


@dg.asset(group_name="evaluations")
def random_forest_metrics(context, random_forest_model, X_test, y_test):
    """Evaluate the Random Forest regression model using MAE and RMSE."""
    preds = random_forest_model.predict(X_test)

    mae = mean_absolute_error(y_test, preds)
    rmse = np.sqrt(mean_squared_error(y_test, preds))

    context.add_output_metadata(
        {
            "model": "RandomForest",
            "mae": float(mae),
            "rmse": float(rmse),
        }
    )

    return {"mae": mae, "rmse": rmse}


@dg.asset(group_name="evaluations")
def xgboost_metrics(context, xgboost_model, X_test, y_test):
    """Evaluate the XGBoost model on the test data."""
    preds = xgboost_model.predict(X_test)

    mae = mean_absolute_error(y_test, preds)
    rmse = np.sqrt(mean_squared_error(y_test, preds))

    context.add_output_metadata(
        {
            "model": "XGBoost",
            "mae": float(mae),
            "rmse": float(rmse),
        }
    )

    return {"mae": mae, "rmse": rmse}
