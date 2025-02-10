#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab                                                                           #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/flow/dataprep/quality/strategy/categorical.py                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 21st 2024 04:33:51 pm                                             #
# Modified   : Saturday February 8th 2025 10:43:01 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Categorical Detect and Repair Strategies"""
from typing import Dict, Type

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from genailab.core.dtypes import DFType
from genailab.flow.dataprep.quality.strategy.factory import (
    DetectStrategy,
    RepairStrategy,
    StrategyFactory,
)


# ------------------------------------------------------------------------------------------------ #
class CategoricalStrategyFactory(StrategyFactory):

    @property
    def detect_strategies(self) -> Dict[str, Type[DetectStrategy]]:
        """
        Retrieves a dictionary of numeric anomaly detection strategies.

        The dictionary maps strategy keys to their corresponding strategy classes.

        Returns:
            Dict[str, Type[DetectStrategy]]: A dictionary where the keys represent
            strategy names, and the values are classes implementing the detection logic.

        Available Detection Strategies:
            - "percentile": ThresholdPercentileAnomalyDetectStrategy
        """
        return {
            "categorical": CategoricalAnomalyDetectRepairTaskDetectStrategy,
        }

    @property
    def repair_strategies(self) -> Dict[str, Type[RepairStrategy]]:
        return {
            "categorical": CategoricalAnomalyDetectRepairTaskRepairStrategy,
        }


# ------------------------------------------------------------------------------------------------ #
class CategoricalAnomalyDetectRepairTaskDetectStrategy(DetectStrategy):
    """
    A strategy for detecting anomalies in categorical data.

    This strategy identifies anomalies by comparing values in a specified column
    against a predefined list of valid categories. Rows with values not in the valid
    categories are flagged as anomalies in a new column.

    Args:
        column (str): The name of the column to evaluate for categorical anomalies.
        new_column (str): The name of the column to store the detection results.
            This column will contain `True` for rows with invalid categories and `False` otherwise.
        valid_categories (list, optional): A list of valid categorical values to compare against.
            Defaults to None, which implies no valid categories are defined.
        **kwargs: Additional keyword arguments for extensibility.

    Methods:
        detect(data: DataFrame) -> DataFrame:
            Detects anomalies by validating the values in the specified column against
            the list of valid categories.
    """

    def __init__(
        self,
        column: str,
        new_column: str,
        valid_categories: list = None,
        **kwargs,
    ) -> None:
        super().__init__()
        self._column = column
        self._new_column = new_column
        self._valid_categories = valid_categories

    def detect(self, data: DFType) -> DFType:
        """
        Detects anomalies in the specified column by validating against the list of valid categories.

        Rows with values not in the list of valid categories are flagged as anomalies,
        with the results stored in a new column.

        Args:
            data (DataFrame): The input PySpark DataFrame to validate.

        Returns:
            DataFrame: The DataFrame with an additional column indicating invalid categories.
                The new column will contain `True` for invalid categories and `False` for valid categories.
        """
        # Add a new column that is True when the value in self._column is not in self._valid_categories
        data = data.withColumn(
            self._new_column, ~F.col(self._column).isin(self._valid_categories)
        )
        return data


# ------------------------------------------------------------------------------------------------ #
class CategoricalAnomalyDetectRepairTaskRepairStrategy(RepairStrategy):
    """
    A strategy for repairing categorical anomalies by removing rows with invalid values.

    This strategy uses a detection strategy to identify rows with invalid categorical
    values based on a predefined list of valid categories. Rows flagged as anomalies
    are removed from the DataFrame.

    Args:
        column (str): The name of the column to evaluate for categorical anomalies.
        new_column (str): The name of the column to store detection results.
            This column will contain `True` for rows with invalid categories and `False` otherwise.
        valid_categories (list, optional): A list of valid categorical values to compare against.
            Defaults to None, which implies no valid categories are defined.
        detect_strategy (Type[CategoricalAnomalyDetectRepairTaskDetectStrategy], optional): The detection
            strategy class to use for identifying anomalies. Defaults to `CategoricalAnomalyDetectRepairTaskDetectStrategy`.
        **kwargs: Additional keyword arguments for extensibility.

    Methods:
        repair(data: DataFrame) -> DataFrame:
            Repairs categorical anomalies by removing rows flagged as anomalies.
    """

    def __init__(
        self,
        column: str,
        new_column: str,
        valid_categories: list = None,
        detect_strategy: Type[
            CategoricalAnomalyDetectRepairTaskDetectStrategy
        ] = CategoricalAnomalyDetectRepairTaskDetectStrategy,
        **kwargs,
    ) -> None:
        self._column = column
        self._new_column = new_column
        self._valid_categories = valid_categories
        self._detect_strategy = detect_strategy

    def repair(self, data: DataFrame) -> DataFrame:
        """
        Repairs categorical anomalies by removing rows with invalid values.

        If the detection results column (`new_column`) does not exist in the DataFrame,
        the detection strategy is applied dynamically to generate it. Rows flagged as
        anomalies in the detection column are then removed.

        Args:
            data (DataFrame): The input PySpark DataFrame to process.

        Returns:
            DataFrame: A new PySpark DataFrame with rows containing invalid categories removed.

        Raises:
            ValueError: If the detection strategy fails to process the data or if the
            anomaly detection column is not generated.
        """
        # Apply detection strategy if the detection column does not exist
        if self._new_column not in data.columns:
            strategy = self._detect_strategy(
                column=self._column,
                new_column=self._new_column,
                valid_categories=self._valid_categories,
            )
            data = strategy.detect(data)

        # Filter out rows where anomalies are detected
        data = data.filter(~F.col(self._new_column))
        return data
