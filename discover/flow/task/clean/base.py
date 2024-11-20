#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/task/clean/base.py                                                   #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday November 20th 2024 04:55:37 pm                                            #
# Modified   : Wednesday November 20th 2024 06:49:16 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import os
import warnings
from abc import abstractmethod
from typing import Optional, Union

import fasttext
import pandas as pd
from lingua import Language, LanguageDetectorBuilder
from pyspark.sql import DataFrame

from discover.flow.task.base import Task

# ------------------------------------------------------------------------------------------------ #
warnings.filterwarnings("ignore")
os.environ["PYTHONWARNINGS"] = "ignore"
# ------------------------------------------------------------------------------------------------ #
#                     LANGUAGE MODELS FOR LANGUAGE DETECTION                                       #
# ------------------------------------------------------------------------------------------------ #
languages = [Language.ENGLISH, Language.SPANISH]
detector = LanguageDetectorBuilder.from_languages(*languages).build()
# ------------------------------------------------------------------------------------------------ #
fasttext.FastText.eprint = lambda x: None  # Suppress FastText warnings
fasttext_model = fasttext.load_model("models/language_detection/lid.176.bin")


# ------------------------------------------------------------------------------------------------ #
#                               ANOMALY DETECTION                                                  #
# ------------------------------------------------------------------------------------------------ #
class Anomaly(Task):
    """Base class for anomalies in data."""

    def __init__(
        self,
        column: str = "content",
        pattern: Optional[str] = None,
        new_column: Optional[str] = None,
        threshold: Optional[int] = None,
        threshold_word_prop: Optional[float] = None,
        threshold_char_prop: Optional[float] = None,
        threshold_percentile: Optional[float] = None,
        **kwargs,
    ):
        """Initialize the anomaly detection task.

        Args:
            column (str): The column to analyze for anomalies.
            pattern (Optional[str]): A regex pattern to detect anomalies.
            new_column (Optional[str]): The name of the column to store detection results.
                Defaults to None, in which case a default name will be assigned.
            threshold (Optional[int]): Absolute threshold for flagging anomalies.
            threshold_word_prop (Optional[float]): Threshold based on word proportion.
            threshold_char_prop (Optional[float]): Threshold based on character proportion.
            threshold_percentile (Optional[float]): Threshold based on percentile.
            **kwargs: Additional arguments for the task.
        """
        super().__init__(**kwargs)
        self._column = column
        self._pattern = pattern
        self._new_column = new_column
        self._threshold = threshold
        self._threshold_word_prop = threshold_word_prop
        self._threshold_char_prop = threshold_char_prop
        self._threshold_percentile = threshold_percentile

    @property
    def detection(self) -> str:
        """The column where detection results are stored."""
        if self._new_column is None:
            raise ValueError(
                "The `new_column` property must be set to access detection results."
            )
        return self._new_column

    @abstractmethod
    def run(
        self, data: Union[pd.DataFrame, DataFrame]
    ) -> Union[pd.DataFrame, DataFrame]:
        """Run the anomaly detection task and store results in the detection column.

        Args:
            data (Union[pd.DataFrame, DataFrame]): The input dataset.

        Returns:
            Union[pd.DataFrame, DataFrame]: The dataset with detection results added.
        """
        pass


# ------------------------------------------------------------------------------------------------ #
#                                 ANOMALY REMOVAL                                                  #
# ------------------------------------------------------------------------------------------------ #
class AnomalyRemoval(Task):
    """Base class for removing anomalies flagged by an anomaly detection task.

    This class is designed to operate in conjunction with an `AnomalyDetection` task.
    It removes rows or records flagged as anomalies based on the detection results.

    Args:
        anomaly_detection (AnomalyDetection): An instance of `AnomalyDetection` that
            provides the detection logic and flags anomalies.
        **kwargs: Additional arguments passed to the parent `Task` class.
    """

    def __init__(
        self,
        anomaly_detection: AnomalyDetection,
        **kwargs,
    ):
        """
        Initialize the anomaly removal task with a detection dependency.

        Args:
            anomaly_detection (AnomalyDetection): The detection task used to identify anomalies.
            **kwargs: Additional arguments for the parent `Task` class.
        """
        super().__init__(**kwargs)
        self._anomaly_detection = (
            anomaly_detection  # Composition: Uses detection as a dependency.
        )

    @abstractmethod
    def run(
        self, data: Union[pd.DataFrame, DataFrame]
    ) -> Union[pd.DataFrame, DataFrame]:
        """
        Abstract method for removing anomalies from the dataset.

        This method must be implemented by subclasses. It typically operates
        by using the `anomaly_detection.detection` property to identify flagged rows
        and removes them from the dataset.

        Args:
            data (Union[pd.DataFrame, DataFrame]): The input dataset to process.

        Returns:
            Union[pd.DataFrame, DataFrame]: The dataset with anomalies removed.

        Raises:
            NotImplementedError: If the method is not implemented in a subclass.
        """
        pass


# ------------------------------------------------------------------------------------------------ #
#                                ANOMALY REPLACEMENT                                               #
# ------------------------------------------------------------------------------------------------ #
class AnomalyReplacement(Task):
    """Base class for replacing anomalies flagged by an anomaly detection task.

    This class works in conjunction with an `AnomalyDetection` task to replace
    anomalies in a dataset with specified replacement values or strategies.

    Args:
        anomaly_detection (AnomalyDetection): An instance of `AnomalyDetection` that
            provides the detection logic and flags anomalies.
        replacement (str): The replacement value or strategy to apply to the flagged anomalies.
        **kwargs: Additional arguments passed to the parent `Task` class.
    """

    def __init__(
        self,
        anomaly_detection: AnomalyDetection,
        replacement: str,
        **kwargs,
    ):
        """
        Initialize the anomaly replacement task with detection and replacement logic.

        Args:
            anomaly_detection (AnomalyDetection): The detection task used to identify anomalies.
            replacement (str): The replacement value or strategy to use for flagged anomalies.
            **kwargs: Additional arguments for the parent `Task` class.
        """
        super().__init__(**kwargs)
        self._anomaly_detection = (
            anomaly_detection  # Composition: Uses detection as a dependency.
        )
        self._replacement = (
            replacement  # Replacement logic (e.g., specific value or method).
        )

    @abstractmethod
    def run(
        self, data: Union[pd.DataFrame, DataFrame]
    ) -> Union[pd.DataFrame, DataFrame]:
        """
        Abstract method for replacing anomalies in the dataset.

        This method must be implemented by subclasses. It typically operates
        by using the `anomaly_detection.detection` property to identify flagged rows
        and applying the specified replacement logic.

        Args:
            data (Union[pd.DataFrame, DataFrame]): The input dataset to process.

        Returns:
            Union[pd.DataFrame, DataFrame]: The dataset with anomalies replaced.

        Raises:
            NotImplementedError: If the method is not implemented in a subclass.
        """
        pass
