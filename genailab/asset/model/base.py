#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab                                                                           #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/asset/model/base.py                                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday January 21st 2025 08:44:42 am                                               #
# Modified   : Saturday February 8th 2025 10:43:31 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""Model Base Module"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
from pyspark.sql import DataFrame

from genailab.core.dstruct import DataClass


# ------------------------------------------------------------------------------------------------ #
#                                      MODEL                                                       #
# ------------------------------------------------------------------------------------------------ #
class Model(ABC):
    """Abstract base class for machine learning models (Sentiment Analysis and ABSA).

    This class provides a structured interface for various machine learning models,
    including methods for data handling, training, prediction, and evaluation.

    Attributes:
        model (Any): The underlying machine learning model instance (e.g., from scikit-learn, transformers).
        model_params (Optional[Dict[str, Any]]): Hyperparameters for the model.

    Methods:
        load_data(self, data_path: str, *args, **kwargs) -> Union[pd.DataFrame, DataFrame, Tuple]: Loads data from a specified source.
        data_split(self, df: Union[pd.DataFrame, DataFrame], *args, **kwargs) -> Tuple: Splits the data into training, validation, and test sets.
        clean_data(self, df: Union[pd.DataFrame, DataFrame], *args, **kwargs) -> Union[pd.DataFrame, DataFrame]: Cleans the input data.
        feature_engineering(self, df: Union[pd.DataFrame, DataFrame], *args, **kwargs) -> Union[pd.DataFrame, DataFrame]: Performs feature engineering.
        feature_scaling(self, df: Union[pd.DataFrame, DataFrame], *args, **kwargs) -> Union[pd.DataFrame, DataFrame]: Scales or normalizes features.
        fit(self, X_train: Any, y_train: Any, *args, **kwargs) -> None: Trains the model.
        partial_fit(self, X_train: Any, y_train: Any, *args, **kwargs) -> None: Performs partial training (for online learning).
        predict(self, X: Any, *args, **kwargs) -> Any: Generates predictions.
        predict_proba(self, X: Any, *args, **kwargs) -> Any: Generates probability predictions (for classification).
        evaluate(self, y_true: Any, y_pred: Any, *args, **kwargs) -> Dict[str, Any]: Evaluates model performance.
        cross_validate(self, X: Any, y: Any, cv: int = 5, *args, **kwargs) -> Dict[str, Any]: Performs cross-validation.
        grid_search(self, X: Any, y: Any, param_grid: Dict[str, List], *args, **kwargs) -> Any: Performs grid search for hyperparameter tuning.
        random_search(self, X: Any, y: Any, param_distributions: Dict[str, Any], n_iter: int = 10, *args, **kwargs) -> Any: Performs random search for hyperparameter tuning.
    """

    def __init__(self, model_params: Optional[Dict[str, Any]] = None):
        """Initializes the Model.

        Args:
            model_params (Optional[Dict[str, Any]]): Hyperparameters for the model.
        """
        self.model = None  # Placeholder for the actual model
        self.model_params = model_params or {}  # Initialize to empty dict if None

    @abstractmethod
    def load_data(
        self, data_path: str, *args, **kwargs
    ) -> Union[pd.DataFrame, DataFrame, Tuple]:
        """Loads data from a specified source."""
        pass

    @abstractmethod
    def data_split(self, df: Union[pd.DataFrame, DataFrame], *args, **kwargs) -> Tuple:
        """Splits the data into training, validation, and test sets."""
        pass

    def clean_data(
        self, df: Union[pd.DataFrame, DataFrame], *args, **kwargs
    ) -> Union[pd.DataFrame, DataFrame]:
        """Cleans the input data. This is a default implementation that does nothing. Subclasses should override this method if cleaning is required."""
        return df

    def feature_engineering(
        self, df: Union[pd.DataFrame, DataFrame], *args, **kwargs
    ) -> Union[pd.DataFrame, DataFrame]:
        """Performs feature engineering. This is a default implementation that does nothing. Subclasses should override this method if feature engineering is required."""
        return df

    def feature_scaling(
        self, df: Union[pd.DataFrame, DataFrame], *args, **kwargs
    ) -> Union[pd.DataFrame, DataFrame]:
        """Scales or normalizes features. This is a default implementation that does nothing. Subclasses should override this method if feature scaling is required."""
        return df

    @abstractmethod
    def fit(self, X_train: Any, y_train: Any, *args, **kwargs) -> None:
        """Trains the model."""
        pass

    def partial_fit(self, X_train: Any, y_train: Any, *args, **kwargs) -> None:
        """Performs partial training (for online learning). This is a default implementation that does nothing. Subclasses should override this method if partial fitting is required."""
        pass

    @abstractmethod
    def predict(self, X: Any, *args, **kwargs) -> Any:
        """Generates predictions."""
        pass

    def predict_proba(self, X: Any, *args, **kwargs) -> Any:
        """Generates probability predictions (for classification). This is a default implementation that raises a NotImplementedError. Subclasses should override this method if probability predictions are required."""
        raise NotImplementedError

    @abstractmethod
    def evaluate(self, y_true: Any, y_pred: Any, *args, **kwargs) -> Dict[str, Any]:
        """Evaluates model performance."""
        pass

    def cross_validate(
        self, X: Any, y: Any, cv: int = 5, *args, **kwargs
    ) -> Dict[str, Any]:
        """Performs cross-validation. This is a default implementation that raises a NotImplementedError. Subclasses should override this method if cross-validation is required."""
        raise NotImplementedError

    def grid_search(
        self, X: Any, y: Any, param_grid: Dict[str, List], *args, **kwargs
    ) -> Any:
        """Performs grid search for hyperparameter tuning. This is a default implementation that raises a NotImplementedError. Subclasses should override this method if grid search is required."""
        raise NotImplementedError

    def random_search(
        self,
        X: Any,
        y: Any,
        param_distributions: Dict[str, Any],
        n_iter: int = 10,
        *args,
        **kwargs,
    ) -> Any:
        """Performs random search for hyperparameter tuning. This is a default implementation that raises a NotImplementedError. Subclasses should override this method if random search is required."""
        raise NotImplementedError


# ------------------------------------------------------------------------------------------------ #
#                                 HYPERPARAMETERS                                                  #
# ------------------------------------------------------------------------------------------------ #
class HyperParameters(DataClass):
    """
    A class to store and manage hyperparameters for a machine learning model.

    Attributes:
        learning_rate (float): The learning rate used during model training.
        batch_size (int): The number of samples used in each training batch.
        num_epochs (int): The number of times the training data is iterated through.
        regularization (float): Regularization strength to prevent overfitting.
        activation (str): Activation function used in the model (e.g., "relu", "sigmoid").
        # ... add more hyperparameters specific to your model type

    Methods:
        to_dict(): Returns a dictionary representation of the hyperparameters.
        update(self, new_params): Updates hyperparameters with new values from a dictionary.
    """

    def __init__(
        self,
        learning_rate=0.01,
        batch_size=32,
        num_epochs=10,
        regularization=0.0,
        activation="relu",
    ):
        self.learning_rate = learning_rate
        self.batch_size = batch_size
        self.num_epochs = num_epochs
        self.regularization = regularization
        self.activation = activation

    def to_dict(self):
        """
        Returns a dictionary representation of the hyperparameters.
        """
        return {key: value for key, value in self.__dict__.items()}

    def update(self, new_params):
        """
        Updates hyperparameters with new values from a dictionary.
        """
        for key, value in new_params.items():
            if hasattr(self, key):
                setattr(self, key, value)
            else:
                print(f"Warning: Unknown hyperparameter '{key}'")
