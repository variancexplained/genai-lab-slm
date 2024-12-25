#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/task/dataprep/clean/strategy/interval.py                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday November 24th 2024 12:21:51 am                                               #
# Modified   : Tuesday December 24th 2024 07:45:40 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from datetime import datetime
from typing import Literal, Type

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

from discover.flow.task.dataprep.clean.strategy.factory import (
    DetectStrategy,
    RepairStrategy,
    StrategyFactory,
)

# ------------------------------------------------------------------------------------------------ #


class IntervalStrategyFactory(StrategyFactory):

    @property
    def detect_strategies(self) -> dict[str, Type[DetectStrategy]]:
        return {
            "date_range": DateRangeAnomalyDetectStrategy,
        }

    @property
    def repair_strategies(self) -> dict[str, Type[RepairStrategy]]:
        return {
            "date_range": DateRangeAnomalyRepairStrategy,
        }


# ------------------------------------------------------------------------------------------------ #
class DateRangeAnomalyDetectStrategy(DetectStrategy):
    """
    Strategy for detecting anomalies in a date range.

    This strategy flags observations where the values in the specified column
    fall outside the defined range for years, months, or specific dates.

    Args:
        column (str): The column to check for anomalies.
        new_column (str): The column to store anomaly flags.
        range_min (int): The minimum value for the date range (integer format).
        range_max (int): The maximum value for the date range (integer format).
        range_type (Literal["year", "month", "date"]): The type of range to apply.
            Options are "year", "month", or "date".
    """

    def __init__(
        self,
        column: str,
        new_column: str,
        range_min: int,
        range_max: int,
        range_type: Literal["year", "month", "date"] = "year",
        **kwargs,
    ) -> None:
        super().__init__()
        self._column = column
        self._new_column = new_column
        self._range_min = self._convert_to_datetime(range_min, range_type)
        self._range_max = self._convert_to_datetime(range_max, range_type)
        self._range_type = range_type

    def _convert_to_datetime(self, value: int, range_type_id: str) -> datetime:
        """
        Converts the integer range value to a datetime object based on the range type.

        Args:
            value (int): The range value to convert.
            range_type (str): The type of range ("year", "month", or "date").

        Returns:
            datetime: The converted datetime object.
        """
        if range_type_id == "year":
            return datetime(value, 1, 1)
        elif range_type_id == "month":
            year = value // 100
            month = value % 100
            return datetime(year, month, 1)
        elif range_type_id == "date":
            year = value // 10000
            month = (value // 100) % 100
            day = value % 100
            return datetime(year, month, day)
        else:
            raise ValueError("Invalid range_type. Must be 'year', 'month', or 'date'.")

    def detect(self, data: DataFrame) -> DataFrame:
        """
        Detects anomalies in the specified column based on the given date range.

        Args:
            data (DataFrame): The input PySpark DataFrame.

        Returns:
            DataFrame: The original DataFrame updated with an additional column (`self._new_column`)
                    indicating anomalies as boolean values.
        """
        # Temporarily cast the column to datetime for comparison
        temp_column = F.col(self._column).cast(DateType())

        # Add the anomaly flag column as a boolean
        data = data.withColumn(
            self._new_column,
            (temp_column < F.lit(self._range_min))
            | (temp_column > F.lit(self._range_max)),
        )

        return data


# ------------------------------------------------------------------------------------------------ #
class DateRangeAnomalyRepairStrategy(RepairStrategy):
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
        column: str,
        new_column: str,
        range_min: int,
        range_max: int,
        range_type: Literal["year", "month", "date"] = "year",
        detect_strategy: Type[
            DateRangeAnomalyDetectStrategy
        ] = DateRangeAnomalyDetectStrategy,
        **kwargs,
    ) -> None:
        super().__init__()
        self._column = column
        self._new_column = new_column
        self._range_min = range_min
        self._range_max = range_max
        self._range_type = range_type
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
                range_type=self._range_type,
            )
            data = strategy.detect(data)

        # Filter out rows where anomalies are detected
        data = data.filter(~F.col(self._new_column))
        return data
