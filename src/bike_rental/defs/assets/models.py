"""Model asset definition."""

import dagster as dg
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from xgboost import XGBRegressor

from bike_rental.defs.assets.helper import metadata_extractor


@dg.asset(group_name="models")
def linear_regression_model(context, X_train, y_train):
    """Train a Linear Regression model on the training data."""
    model = LinearRegression()

    model.fit(X_train, y_train)

    context.add_output_metadata(
        {
            "model_type": "LinearRegression",
            "n_features": X_train.shape[1],
            "train_rows": len(X_train),
        }
        | metadata_extractor(X_train)
    )

    return model


@dg.asset(group_name="models")
def random_forest_model(context, X_train, y_train):
    """Train a Random Forest regression model on the training data."""
    model = RandomForestRegressor(
        n_estimators=200, max_depth=None, random_state=42, n_jobs=-1
    )

    model.fit(X_train, y_train)

    context.add_output_metadata(
        {
            "model_type": "RandomForest",
            "n_estimators": 200,
            "train_rows": len(X_train),
        }
        | metadata_extractor(X_train)
    )

    return model


@dg.asset(group_name="models")
def xgboost_model(context, X_train, y_train):
    """Train an XGBoost regression model on the training data."""
    model = XGBRegressor(
        n_estimators=300,
        learning_rate=0.05,
        max_depth=6,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
    )

    model.fit(X_train, y_train)

    context.add_output_metadata(
        {
            "model_type": "XGBoost",
            "n_estimators": 300,
            "learning_rate": 0.05,
            "train_rows": len(X_train),
        }
        | metadata_extractor(X_train)
    )

    return model
