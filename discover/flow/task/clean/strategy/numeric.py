#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/task/clean/strategy/numeric.py                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 21st 2024 02:58:58 pm                                             #
# Modified   : Monday December 23rd 2024 04:20:32 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Numeric Detect and Repair Strategies"""
from abc import abstractmethod
from typing import Optional, Type, Union

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from discover.core.data_structure import DataFrameStructure
from discover.flow.task.clean.strategy.factory import (
    DetectStrategy,
    RepairStrategy,
    StrategyFactory,
)


# ------------------------------------------------------------------------------------------------ #
class NumericStrategyFactory(StrategyFactory):
    """
    A factory class to retrieve strategies for numeric anomaly detection and repair.

    This factory provides mappings for numeric anomaly detection and repair strategies,
    allowing the user to select and instantiate appropriate strategies based on a key.

    Methods:
        detect_strategies (property): Returns a dictionary mapping keys to numeric anomaly
            detection strategies.
        repair_strategies (property): Returns a dictionary mapping keys to numeric anomaly
            repair strategies.
    """

    @property
    def detect_strategies(self) -> dict[str, Type[DetectStrategy]]:
        """
        Retrieves a dictionary of numeric anomaly detection strategies.

        The dictionary maps strategy keys to their corresponding strategy classes.

        Returns:
            dict[str, Type[DetectStrategy]]: A dictionary where the keys represent
            strategy names, and the values are classes implementing the detection logic.

        Available Detection Strategies:
            - "percentile": ThresholdPercentileAnomalyDetectStrategy
        """
        return {
            "percentile": ThresholdPercentileAnomalyDetectStrategy,
            "threshold": ThresholdAnomalyDetectStrategy,
        }

    @property
    def repair_strategies(self) -> dict[str, Type[RepairStrategy]]:
        """
        Retrieves a dictionary of numeric anomaly repair strategies.

        The dictionary maps strategy keys to their corresponding strategy classes.

        Returns:
            dict[str, Type[RepairStrategy]]: A dictionary where the keys represent
            strategy names, and the values are classes implementing the repair logic.

        Available Repair Strategies:
            - "percentile": ThresholdPercentileAnomalyRepairStrategy
        """
        return {
            "percentile": ThresholdPercentileAnomalyRepairStrategy,
            "threshold": ThresholdAnomalyRepairStrategy,
        }


# ------------------------------------------------------------------------------------------------ #
class NumericDetectStrategy(DetectStrategy):

    def __init__(
        self,
        column: str,
        new_column: str,
        threshold: Optional[Union[int, float]] = None,
        detect_less_than_threshold: Optional[bool] = True,
        **kwargs,
    ) -> None:
        super().__init__()
        self._column = column
        self._new_column = new_column
        self._threshold = threshold
        self._detect_less_than_threshold = detect_less_than_threshold

    @abstractmethod
    def detect(self, data: DataFrameStructure) -> DataFrameStructure:
        """
        Detects anomalies in the provided data.

        Args:
            data: The input data to analyze for anomalies. The type of data
                (e.g., string, list, DataFrame) depends on the specific implementation.

        Returns:
            The result of the detection process, typically a boolean indicator,
            a list of flagged rows, or a DataFrame with a detection column.

        Raises:
            NotImplementedError: If the method is not implemented by a subclass.
        """
        pass


# ------------------------------------------------------------------------------------------------ #
class NumericRepairStrategy(RepairStrategy):

    def __init__(
        self,
        column: str,
        new_column: str,
        threshold: Optional[Union[int, float]] = None,
        detect_less_than_threshold: Optional[bool] = True,
        **kwargs,
    ) -> None:
        super().__init__()
        self._column = column
        self._new_column = new_column
        self._threshold = threshold
        self._detect_less_than_threshold = detect_less_than_threshold

    @abstractmethod
    def repair(self, data: DataFrameStructure) -> DataFrameStructure:
        """
        Detects anomalies in the provided data.

        Args:
            data: The input data to analyze for anomalies. The type of data
                (e.g., string, list, DataFrame) depends on the specific implementation.

        Returns:
            The result of the detection process, typically a boolean indicator,
            a list of flagged rows, or a DataFrame with a detection column.

        Raises:
            NotImplementedError: If the method is not implemented by a subclass.
        """
        pass


# ------------------------------------------------------------------------------------------------ #
class ThresholdPercentileAnomalyDetectStrategy(NumericDetectStrategy):
    """
    A strategy for detecting numeric anomalies using percentile-based thresholds.

    This strategy identifies anomalies in a numeric column by comparing values against
    a computed percentile threshold. The threshold is computed using the `approxQuantile`
    method for efficient and approximate percentile calculation. Users can specify whether
    to detect values below or above the threshold.

    Args:
        column (str): The name of the column to evaluate for anomalies.
        new_column (str): The name of the column to store detection results.
        threshold (float, optional): The percentile threshold for anomaly detection.
            Defaults to 0.5 (50th percentile, median).
        relative_error (float, optional): The relative error for the `approxQuantile` calculation.
            Smaller values result in more precise thresholds but may increase computation time.
            Defaults to 0.001.
        detect_less_than_threshold (bool, optional): If True, detects values below the threshold.
            If False, detects values above the threshold. Defaults to True.
        **kwargs: Additional keyword arguments for extensibility.

    Methods:
        detect(data: DataFrame) -> DataFrame:
            Detects anomalies in the specified column based on the computed percentile threshold.
    """

    def __init__(
        self,
        column: str,
        new_column: str,
        threshold: float = 0.5,
        relative_error: float = 0.001,
        detect_less_than_threshold: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(
            column=column,
            new_column=new_column,
            threshold=threshold,
            detect_less_than_threshold=detect_less_than_threshold,
            relative_error=relative_error,
            **kwargs,
        )

    def detect(self, data: DataFrame) -> DataFrame:
        """
        Detects anomalies in the specified column based on a computed percentile threshold.

        The percentile threshold is computed using the `approxQuantile` method for approximate
        calculation with controllable relative error. Rows are flagged as anomalies if their
        values fall below or above the threshold, depending on the `detect_less_than_threshold` parameter.

        Args:
            data (DataFrame): The input PySpark DataFrame to analyze.

        Returns:
            DataFrame: The DataFrame with an additional column containing detection results
            (True for anomalies, False otherwise).

        Raises:
            ValueError: If the computed percentile threshold is null, indicating potential
            issues with the data or configuration.
        """
        # Compute the percentile threshold with a smaller relative error for better precision
        threshold = data.approxQuantile(
            self._column, [self._threshold], self._relative_error
        )[0]

        # Check for potential null threshold and raise an error if necessary
        if threshold is None:
            raise ValueError(
                "Computed percentile threshold is null. Consider adjusting the relative error or data quality."
            )

        # Apply the threshold logic to the DataFrame
        if self._detect_less_than_threshold:
            data = data.withColumn(
                self._new_column,
                F.when(F.col(self._column) < F.lit(threshold), True).otherwise(False),
            )
        else:
            data = data.withColumn(
                self._new_column,
                F.when(F.col(self._column) > F.lit(threshold), True).otherwise(False),
            )
        return data


# ------------------------------------------------------------------------------------------------ #
class ThresholdPercentileAnomalyRepairStrategy(NumericRepairStrategy):
    """
    A strategy for repairing numeric anomalies by removing rows based on percentile-based thresholds.

    This strategy identifies and removes rows with numeric anomalies using a detection strategy
    based on a computed percentile threshold. The detection strategy can be applied dynamically
    if the detection column does not already exist in the DataFrame.

    Methods:
        repair(data: DataFrame) -> DataFrame:
            Repairs anomalies in the specified column by removing rows flagged as anomalies.
    """

    def __init__(
        self,
        column: str,
        new_column: str,
        threshold: float = 0.5,
        relative_error: float = 0.001,
        detect_less_than_threshold: bool = True,
        detect_strategy: Type[
            ThresholdPercentileAnomalyDetectStrategy
        ] = ThresholdPercentileAnomalyDetectStrategy,
        **kwargs,
    ):
        super().__init__(
            column=column,
            new_column=new_column,
            threshold=threshold,
            detect_less_than_threshold=detect_less_than_threshold,
            relative_error=relative_error,
            **kwargs,
        )
        self._detect_strategy = detect_strategy

    def repair(self, data: DataFrame) -> DataFrame:
        """
        Repairs numeric anomalies by removing rows flagged by a detection strategy.

        If the detection column (`new_column`) does not exist in the DataFrame,
        the specified detection strategy is applied dynamically to generate it.
        Rows flagged as anomalies in the detection column are then removed.

        Args:
            data (DataFrame): The input PySpark DataFrame to process.

        Returns:
            DataFrame: A new PySpark DataFrame with rows containing anomalies removed.

        Raises:
            ValueError: If the detection strategy fails to process the data or
            if the anomaly detection column is not generated.
        """
        # Apply detection strategy if the detection column does not exist
        if self._new_column not in data.columns:
            strategy = self._detect_strategy(
                column=self._column,
                new_column=self._new_column,
                threshold=self._threshold,
                relative_error=self._relative_error,
                detect_less_than_threshold=self._detect_less_than_threshold,
            )
            data = strategy.detect(data)

        # Filter out rows where anomalies are detected
        data = data.filter(~F.col(self._new_column))
        return data


# ------------------------------------------------------------------------------------------------ #
class ThresholdAnomalyDetectStrategy(NumericDetectStrategy):
    """
    A strategy for detecting numeric anomalies based on a fixed threshold.

    This strategy identifies anomalies by comparing values in a specified column
    against a fixed threshold. Users can configure the logic to detect values
    either below or above the threshold.

    Args:
        column (str): The name of the column to evaluate for anomalies.
        new_column (str): The name of the column to store detection results.
            This column will contain `True` for rows that meet the anomaly condition
            and `False` otherwise.
        threshold (Union[int, float], optional): The fixed threshold value for anomaly detection.
            Defaults to None, which implies no threshold is applied.
        detect_less_than_threshold (bool, optional): If True, detects values less than the threshold.
            If False, detects values greater than the threshold. Defaults to True.
        **kwargs: Additional keyword arguments for extensibility.

    Methods:
        detect(data: DataFrame) -> DataFrame:
            Detects numeric anomalies by applying the threshold logic to the specified column.
    """

    def __init__(
        self,
        column: str,
        new_column: str,
        threshold: Union[int, float] = None,
        detect_less_than_threshold: bool = True,
        **kwargs,
    ) -> None:
        """
        Initializes the threshold-based numeric anomaly detection strategy.

        Args:
            column (str): The name of the column to evaluate for anomalies.
            new_column (str): The name of the column to store detection results.
            threshold (Union[int, float], optional): The fixed threshold value for anomaly detection.
                Defaults to None.
            detect_less_than_threshold (bool, optional): If True, detects values less than the threshold.
                If False, detects values greater than the threshold. Defaults to True.
            **kwargs: Additional keyword arguments for extensibility.
        """
        super().__init__(
            column=column,
            new_column=new_column,
            threshold=threshold,
            detect_less_than_threshold=detect_less_than_threshold,
            **kwargs,
        )

    def detect(self, data: DataFrame) -> DataFrame:
        """
        Detects numeric anomalies by applying the threshold logic.

        Rows with values below or above the specified threshold (based on the
        `detect_less_than_threshold` parameter) are flagged as anomalies. The
        results are stored in a new column with `True` for anomalies and `False`
        for valid values.

        Args:
            data (DataFrame): The input PySpark DataFrame to analyze.

        Returns:
            DataFrame: A PySpark DataFrame with an additional column indicating anomalies.
        """
        # Apply the threshold logic to the DataFrame
        if self._detect_less_than_threshold:
            data = data.withColumn(
                self._new_column,
                F.when(F.col(self._column) < F.lit(self._threshold), True).otherwise(
                    False
                ),
            )
        else:
            data = data.withColumn(
                self._new_column,
                F.when(F.col(self._column) > F.lit(self._threshold), True).otherwise(
                    False
                ),
            )
        return data


# ------------------------------------------------------------------------------------------------ #
class ThresholdAnomalyRepairStrategy(NumericRepairStrategy):
    """
    A strategy for repairing numeric anomalies by removing rows based on a fixed threshold.

    This strategy identifies numeric anomalies using a detection strategy based on a
    fixed threshold and removes rows flagged as anomalies. Users can configure the threshold
    and logic for detecting values either below or above the threshold.

    Args:
        column (str): The name of the column to evaluate for anomalies.
        new_column (str): The name of the column to store detection results.
            This column will contain `True` for rows that meet the anomaly condition
            and `False` otherwise.
        threshold (Union[int, float], optional): The fixed threshold value for anomaly detection.
            Defaults to None, which implies no threshold is applied.
        detect_less_than_threshold (bool, optional): If True, detects values less than the threshold.
            If False, detects values greater than the threshold. Defaults to True.
        detect_strategy (Type[ThresholdAnomalyDetectStrategy], optional): The detection strategy
            class to use for identifying anomalies. Defaults to `ThresholdAnomalyDetectStrategy`.
        **kwargs: Additional keyword arguments for extensibility.

    Methods:
        repair(data: DataFrame) -> DataFrame:
            Repairs numeric anomalies by removing rows flagged as anomalies.
    """

    def __init__(
        self,
        column: str,
        new_column: str,
        threshold: Union[int, float] = None,
        detect_less_than_threshold: bool = True,
        detect_strategy: Type[
            ThresholdAnomalyDetectStrategy
        ] = ThresholdAnomalyDetectStrategy,
        **kwargs,
    ):
        """
        Initializes the threshold-based numeric anomaly repair strategy.

        Args:
            column (str): The name of the column to evaluate for anomalies.
            new_column (str): The name of the column to store detection results.
            threshold (Union[int, float], optional): The fixed threshold value for anomaly detection.
                Defaults to None.
            detect_less_than_threshold (bool, optional): If True, detects values less than the threshold.
                If False, detects values greater than the threshold. Defaults to True.
            detect_strategy (Type[ThresholdAnomalyDetectStrategy], optional): The detection strategy
                class to use for identifying anomalies. Defaults to `ThresholdAnomalyDetectStrategy`.
            **kwargs: Additional keyword arguments for extensibility.
        """
        super().__init__(
            column=column,
            new_column=new_column,
            threshold=threshold,
            detect_less_than_threshold=detect_less_than_threshold,
            **kwargs,
        )
        self._detect_strategy = detect_strategy

    def repair(self, data: DataFrame) -> DataFrame:
        """
        Repairs numeric anomalies by removing rows flagged as anomalies.

        If the detection results column (`new_column`) does not exist in the DataFrame,
        the detection strategy is applied dynamically to generate it. Rows flagged as
        anomalies in the detection column are then removed.

        Args:
            data (DataFrame): The input PySpark DataFrame to process.

        Returns:
            DataFrame: A PySpark DataFrame with rows containing anomalies removed.

        Raises:
            ValueError: If the detection strategy fails to process the data or if the
            anomaly detection column is not generated.
        """
        # Apply detection strategy if the detection column does not exist
        if self._new_column not in data.columns:
            strategy = self._detect_strategy(
                column=self._column,
                new_column=self._new_column,
                threshold=self._threshold,
                detect_less_than_threshold=self._detect_less_than_threshold,
            )
            data = strategy.detect(data)

        # Filter out rows where anomalies are detected
        data = data.filter(~F.col(self._new_column))
        return data
