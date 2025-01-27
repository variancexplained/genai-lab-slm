#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/asset/experiment/base.py                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday January 21st 2025 10:06:04 am                                               #
# Modified   : Sunday January 26th 2025 10:38:16 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from genailab.asset.dataset.dataset import Dataset
from genailab.asset.dataset.identity import DatasetPassport
from genailab.asset.experiment.identity import ExperimentPassport
from genailab.asset.model.base import HyperParameters, Model
from genailab.core.dstruct import DataClass
from pydantic.dataclasses import dataclass


# ------------------------------------------------------------------------------------------------ #
#                                EXPERIMENT DATASETS CLASS                                         #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class ExperimentDatasets(DataClass):
    train: DatasetPassport
    validation: DatasetPassport
    test: DatasetPassport


# ------------------------------------------------------------------------------------------------ #
#                                       EXPERIMENT                                                 #
# ------------------------------------------------------------------------------------------------ #
class Experiment(ABC):
    """Abstract base class for machine learning experiments.

    This class provides a structured way to manage and track machine learning
    experiments, including the models, data, activities (e.g., training, evaluation),
    and results.

    Attributes:
        name (str): The name of the experiment.
        description (Optional[str]): A description of the experiment.
        model (Optional[Model]): The machine learning model used in the experiment.
        datasets (Optional[Dict[str, Dataset]]): A dictionary of datasets used in the experiment, keyed by name (e.g., "train", "test").
        results (Optional[Dict[str, Any]]): A dictionary to store experiment results (e.g., metrics, predictions).
        activities (Optional[List[str]]): A list of activities performed in the experiment (e.g., "training", "evaluation").

    Methods:
        setup(self, *args, **kwargs) -> None: Sets up the experiment environment (e.g., loading data, initializing models).
        run(self, *args, **kwargs) -> None: Executes the experiment.
        log_result(self, key: str, value: Any) -> None: Logs a result from the experiment.
        log_activity(self, activity: str) -> None: Logs an activity performed during the experiment.
        get_results(self) -> Dict[str, Any]: Returns the experiment results.
        get_activities(self) -> List[str]: Returns the list of activities performed.
        get_dataset(self, name: str) -> Optional[Dataset]: Retrieves a dataset by name.
        get_model(self) -> Optional[Model]: Retrieves the model used in the experiment.
    """

    def __init__(
        self,
        name: str,
        datasets: ExperimentDatasets,
        model: Model,
        hyperperamaters: HyperParameters,
        description: Optional[str] = None,
    ):
        """Initializes an Experiment object.

        Args:
            name (str): The name of the experiment.
            description (Optional[str]): A description of the experiment.
            model (Optional[Model]): The machine learning model used in the experiment.
            datasets (Optional[Dict[str, Dataset]]): A dictionary of datasets used in the experiment.
        """
        self._name = name
        self._description = description
        self._model = model
        self._datasets = datasets
        self._hyperparameters = HyperParameters

        self._results: Dict[str, Any] = {}
        self._passport = None
        self._fileset = None

    @property
    def passport(self) -> ExperimentPassport:
        return self._passport

    @property
    def datasets(self) -> ExperimentDatasets:
        return self._datasets

    @property
    def model(self) -> Model:
        return self._model

    @property
    def hyperparameters(self) -> HyperParameters:
        return self._hyperparameters

    @abstractmethod
    def preprocess_data(self, *args, **kwargs) -> None:
        """Preprocesses the data for training."""
        pass

    @abstractmethod
    def train(self, *args, **kwargs) -> None:
        """Trains the model."""
        pass

    @abstractmethod
    def evaluate(self, *args, **kwargs) -> None:
        """Evaluate the model"""

    def get_results(self) -> Dict[str, Any]:
        """Returns the experiment results.

        Returns:
            Dict[str, Any]: A dictionary of experiment results.
        """
        return self.results

    def get_activities(self) -> List[str]:
        """Returns the list of activities performed.

        Returns:
            List[str]: A list of activity names.
        """
        return self.activities

    def get_dataset(self, name: str) -> Optional[Dataset]:
        """Retrieves a dataset by name.

        Args:
            name (str): The name of the dataset to retrieve.

        Returns:
            Optional[Dataset]: The dataset if found, otherwise None.
        """
        return self.datasets.get(name)

    def get_model(self) -> Optional[Model]:
        """Retrieves the model used in the experiment.

        Returns:
            Optional[Model]: The model used in the experiment, or None if no model is set.
        """
        return self.model
