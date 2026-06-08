"""Utilities for fitting and evaluating predictive models."""

from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    r2_score,
)


class Trainer:
    """Wrap a model object with fit, predict, and evaluation helpers."""

    def __init__(self, model: Any) -> None:
        """Initialize the trainer.

        Parameters
        ----------
        model
            A scikit-learn compatible estimator.

        """
        self.model = model

    def fit(self, X_train: pd.DataFrame, y_train: pd.Series) -> None:
        """Fit the wrapped model on the provided training data.

        Parameters
        ----------
        X_train
            Training features.
        y_train
            Training target values.

        """
        self.model.fit(X_train, y_train)

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """Generate predictions for the provided feature matrix.

        Parameters
        ----------
        X
            Feature matrix to score.

        Returns
        -------
        np.ndarray
            Model predictions.

        """
        return self.model.predict(X)

    def evaluate(
        self, X_test: pd.DataFrame, y_test: pd.Series
    ) -> dict[str, float]:
        """Compute regression metrics on the provided test data.

        Parameters
        ----------
        X_test
            Test features.
        y_test
            Ground-truth target values.

        Returns
        -------
        dict[str, float]
            Dictionary with MAE, RMSE, and R2 scores.

        """
        y_pred = self.predict(X_test)

        return {
            "mae": mean_absolute_error(y_test, y_pred),
            "rmse": np.sqrt(mean_squared_error(y_test, y_pred)),
            "r2": r2_score(y_test, y_pred),
        }
