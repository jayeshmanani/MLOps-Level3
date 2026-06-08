"""Model factory utilities for the bike rental project."""

from collections.abc import Mapping
from enum import Enum
from typing import Any

from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from xgboost import XGBRegressor


class ModelType(Enum):
    """Supported regression model types."""

    LINEAR_REGRESSION = "linear_regression"
    RANDOM_FOREST = "random_forest"
    XGBOOST = "xgboost"


class ModelFactory:
    """Factory for creating model instances from a `ModelType` key."""

    MODELS = {
        ModelType.LINEAR_REGRESSION: LinearRegression,
        ModelType.RANDOM_FOREST: RandomForestRegressor,
        ModelType.XGBOOST: XGBRegressor,
    }

    @classmethod
    def create(
        cls,
        model_name: str | ModelType,
        params: Mapping[str, Any] | None = None,
    ) -> Any:
        """Create a configured model instance.

        Parameters
        ----------
        model_name
            Model identifier.
        params
            Optional keyword arguments for the model constructor.

        Returns
        -------
        Any
            Instantiated regression model.

        Raises
        ------
        ValueError
            If the requested model is not supported.

        """
        model_key = (
            ModelType(model_name)
            if not isinstance(model_name, ModelType)
            else model_name
        )

        if model_key not in cls.MODELS:
            raise ValueError(
                f"Unsupported model '{model_name}'. "
                f"Available models: {list(cls.MODELS.keys())}"
            )

        params = dict(params or {})

        return cls.MODELS[model_key](**params)
