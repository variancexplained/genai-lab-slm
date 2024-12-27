#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/task/dataprep/clean/strategy/nominal.py                              #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 21st 2024 04:34:11 pm                                             #
# Modified   : Friday December 27th 2024 10:35:08 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Nominal Value Detect and Repair Strategies"""
from abc import abstractmethod
from typing import Type

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from discover.core.dataset import DataFrameStructureEnum
from discover.flow.task.dataprep.clean.strategy.factory import (
    DetectStrategy,
    RepairStrategy,
    StrategyFactory,
)


# ------------------------------------------------------------------------------------------------ #
class NominalStrategyFactory(StrategyFactory):
    """
    Factory for creating strategies to detect and repair nominal anomalies.

    This factory provides mappings between strategy names and their corresponding
    implementations for detecting and repairing nominal anomalies. It extends the
    `StrategyFactory` base class and defines the available strategies for nominal
    anomaly detection and repair.

    Methods:
        detect_strategies -> dict[str, Type[DetectStrategy]]:
            Returns a dictionary mapping strategy names to their respective
            detection strategy classes.

        repair_strategies -> dict[str, Type[RepairStrategy]]:
            Returns a dictionary mapping strategy names to their respective
            repair strategy classes.
    """

    @property
    def detect_strategies(self) -> dict[str, Type[DetectStrategy]]:
        return {
            "nominal": NominalAnomalyDetectRepairTaskDetectStrategy,
            "unique": UniquenessAnomalyDetectStrategy,
        }

    @property
    def repair_strategies(self) -> dict[str, Type[RepairStrategy]]:
        return {
            "nominal": NominalAnomalyDetectRepairTaskRepairStrategy,
            "unique": UniquenessAnomalyRepairStrategy,
        }


# ------------------------------------------------------------------------------------------------ #
class NominalAnomalyDetectRepairTaskDetectStrategy(DetectStrategy):
    """
    Strategy for detecting nominal anomalies in a dataset.

    This class provides an abstract base for implementing detection strategies
    for nominal anomalies. It requires subclasses to define the `detect` method,
    which applies the detection logic to the input dataset.

    Methods:
        detect(data: DataFrameStructureEnum) -> DataFrameStructureEnum:
            Abstract method that applies the detection logic to the provided dataset.

    Notes:
        - The `column` attribute specifies the input column to analyze.
        - The `new_column` attribute specifies the column where detection results
          will be stored.
    """

    def __init__(
        self,
        column: str,
        new_column: str,
        **kwargs,
    ) -> None:

        super().__init__()
        self._column = column
        self._new_column = new_column

    @abstractmethod
    def detect(self, data: DataFrameStructureEnum) -> DataFrameStructureEnum:
        """
        Applies anomaly detection logic to the provided dataset.

        Args:
            data (DataFrameStructureEnum): The input dataset to analyze for nominal anomalies.

        Returns:
            DataFrameStructureEnum: The dataset with anomaly detection results added.

        Raises:
            NotImplementedError: If the method is not implemented in a subclass.
        """
        pass


# ------------------------------------------------------------------------------------------------ #
class NominalAnomalyDetectRepairTaskRepairStrategy(RepairStrategy):
    """
    Strategy for repairing nominal anomalies in a dataset.

    This abstract class provides a base for implementing strategies to repair
    nominal anomalies. It uses a specified detection strategy to identify
    anomalies and repair them in the target column.

    Attributes:
        _column (str): The name of the column containing the data to repair.
        _new_column (str): The name of the column where the repaired data will be stored.
        _detect_strategy (Type[NominalAnomalyDetectRepairTaskDetectStrategy]): The detection strategy
            used to identify nominal anomalies.
    """

    def __init__(
        self,
        column: str,
        new_column: str,
        detect_strategy: Type[NominalAnomalyDetectRepairTaskDetectStrategy],
        **kwargs,
    ) -> None:
        super().__init__()
        self._column = column
        self._new_column = new_column
        self._detect_strategy = detect_strategy

    @abstractmethod
    def repair(self, data: DataFrame) -> DataFrame:
        """
        Repairs anomalies in the target column of the dataset.

        Args:
            data (DataFrame): The input dataset containing the target column.

        Returns:
            DataFrame: The dataset with the repaired values added to the specified new column.

        Raises:
            NotImplementedError: If the method is not implemented in a subclass.
        """
        pass


# ------------------------------------------------------------------------------------------------ #
class UniquenessAnomalyDetectStrategy(NominalAnomalyDetectRepairTaskDetectStrategy):
    """
    Strategy for detecting uniqueness anomalies in a dataset.

    This strategy marks rows as duplicates if there are multiple occurrences
    of the same combination of values in the specified columns (`self._columns`).
    Rows are sorted by a "date" column, and only the last occurrence is marked
    as unique, with all others marked as duplicates.

    Args:
        column (list[str]): A list of column names to include in duplication evaluation.
        new_column (str): Name of the column to store duplicate indicators.
    """

    def __init__(
        self,
        column: list[str],
        new_column: str,
        **kwargs,
    ) -> None:
        super().__init__(column=column, new_column=new_column)

    def detect(self, data: DataFrame) -> DataFrame:
        """
        Detects duplicate rows in the dataset based on the specified columns.

        Args:
            data (DataFrame): The input PySpark DataFrame.

        Returns:
            DataFrame: A DataFrame with an additional column (`self._new_column`)
                       indicating duplicates. Rows with 1 are duplicates, and
                       rows with 0 are unique.
        """
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        # Define a window specification partitioned by the target columns and ordered by date
        window_spec = Window.partitionBy(*self._column).orderBy(F.desc("date"))

        # Add a row number to identify duplicates
        data_with_duplicates = data.withColumn(
            "row_number", F.row_number().over(window_spec)
        )

        # Mark duplicates: True for duplicates, False for the last (unique) occurrence
        result = data_with_duplicates.withColumn(
            self._new_column, F.when(F.col("row_number") > 1, True).otherwise(False)
        ).drop(
            "row_number"
        )  # Optionally drop the intermediate column

        return result


# ------------------------------------------------------------------------------------------------ #
class UniquenessAnomalyRepairStrategy(NominalAnomalyDetectRepairTaskRepairStrategy):
    """
    Strategy for repairing uniqueness anomalies in a dataset.

    This class repairs anomalies by ensuring that only the last occurrence
    of duplicate values (based on the specified column) is retained. It uses
    a detection strategy to mark duplicates and filters out all but the last
    occurrence.

    Args:
        column (list[str]): List of columns to consider for uniqueness.
        new_column (str): The name of the column where the detection results will be stored.
        detect_strategy (Type[UniquenessAnomalyDetectStrategy]): The strategy used to detect uniqueness anomalies.
        **kwargs: Additional keyword arguments passed to the base class.
    """

    def __init__(
        self,
        column: list[str],
        new_column: str,
        detect_strategy: Type[
            UniquenessAnomalyDetectStrategy
        ] = UniquenessAnomalyDetectStrategy,
        **kwargs,
    ) -> None:
        super().__init__(
            column=column, new_column=new_column, detect_strategy=detect_strategy
        )

    def repair(self, data: DataFrame) -> DataFrame:
        """
        Repairs uniqueness anomalies in the dataset.

        If the detection column (`self._new_column`) is not present, the method
        applies the detection strategy to identify duplicate rows based on
        `self._column`. It then filters out all but the last occurrence of
        duplicates, keeping the most recent entry.

        Args:
            data (DataFrame): The input PySpark DataFrame.

        Returns:
            DataFrame: A DataFrame with uniqueness anomalies repaired. Rows with
            duplicate values in `self._column` are removed except for the last
            occurrence.

        Raises:
            ValueError: If the detection strategy fails to add the detection column.

        Notes:
            - If `self._new_column` does not exist, the method first applies the
              detection strategy to create it.
            - The filtering is performed based on the values in `self._new_column`.
        """
        # Apply detection strategy if the detection column does not exist
        if self._new_column not in data.columns:
            strategy = self._detect_strategy(
                column=self._column,
                new_column=self._new_column,
            )
            data = strategy.detect(data)

        # Filter out rows where anomalies are detected
        data = data.filter(~F.col(self._new_column))
        return data
