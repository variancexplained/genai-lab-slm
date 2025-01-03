#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/dataprep/quality/strategy/discrete.py                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday November 24th 2024 01:04:01 am                                               #
# Modified   : Friday January 3rd 2025 12:59:35 am                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from typing import Dict, Type

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from discover.flow.dataprep.quality.strategy.factory import (
    DetectStrategy,
    RepairStrategy,
    StrategyFactory,
)

# ------------------------------------------------------------------------------------------------ #


class DiscreteStrategyFactory(StrategyFactory):

    @property
    def detect_strategies(self) -> Dict[str, Type[DetectStrategy]]:
        return {
            "range": DiscreteRangeAnomalyDetectStrategy,
        }

    @property
    def repair_strategies(self) -> Dict[str, Type[RepairStrategy]]:
        return {
            "range": DateRangeAnomalyRepairStrategy,
        }


# ------------------------------------------------------------------------------------------------ #
class DiscreteRangeAnomalyDetectStrategy(DetectStrategy):
    def __init__(
        self,
        column: str,
        new_column: str,
        range_min: int,
        range_max: int,
        **kwargs,
    ) -> None:
        super().__init__()
        self._column = column
        self._new_column = new_column
        self._range_min = range_min
        self._range_max = range_max

    def detect(self, data: DataFrame) -> DataFrame:
        """
        Detects rows where the specified column has non-integer values or
        integer values outside the specified range [range_min, range_max].

        Args:
            data (DataFrame): The input PySpark DataFrame.

        Returns:
            DataFrame: A DataFrame with an additional column (`self._new_column`)
                       indicating anomalies. Rows with True are anomalies, and
                       rows with False are valid.
        """
        # Check if the column value is an integer
        is_integer = F.col(self._column).rlike(r"^-?\d+$")

        # Check if the column value is within the range
        in_range = (F.col(self._column).cast("int") >= self._range_min) & (
            F.col(self._column).cast("int") <= self._range_max
        )

        # Flag anomalies: Non-integer values or integers outside the range
        anomaly_flag = ~is_integer | ~in_range

        # Add the anomaly flag column
        return data.withColumn(self._new_column, anomaly_flag)


# ------------------------------------------------------------------------------------------------ #
class DateRangeAnomalyRepairStrategy(RepairStrategy):
    """
    Strategy for repairing uniqueness anomalies in a dataset.

    This class repairs anomalies by ensuring that only the last occurrence
    of duplicate values (based on the specified column) is retained. It uses
    a detection strategy to mark duplicates and filters out all but the last
    occurrence.

    Args:
        column (List[str]): List of columns to consider for uniqueness.
        new_column (str): The name of the column where the detection results will be stored.
        detect_strategy (Type[UniquenessAnomalyDetectStrategy]): The strategy used to detect uniqueness anomalies.
        **kwargs: Additional keyword arguments passed to the base class.
    """

    def __init__(
        self,
        column: str,
        new_column: str,
        range_min: int,
        range_max: int,
        detect_strategy: Type[
            DiscreteRangeAnomalyDetectStrategy
        ] = DiscreteRangeAnomalyDetectStrategy,
        **kwargs,
    ) -> None:
        super().__init__()
        self._column = column
        self._new_column = new_column
        self._range_min = range_min
        self._range_max = range_max
        self._detect_strategy = detect_strategy

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
                range_min=self._range_min,
                range_max=self._range_max,
            )
            data = strategy.detect(data)

        # Filter out rows where anomalies are detected
        data = data.filter(~F.col(self._new_column))
        return data
