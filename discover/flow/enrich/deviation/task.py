#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/enrich/deviation/task.py                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 7th 2024 11:25:40 pm                                              #
# Modified   : Saturday November 9th 2024 02:06:01 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Percent Deviation Module"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from discover.flow.base.task import Task
from discover.infra.service.logging.task import task_logger


# ------------------------------------------------------------------------------------------------ #
class ComputePercentDeviationTask(Task):
    """
    A task to compute the percent deviation of a specified column from the average
    grouped by a given level and store the result in a new column.

    Attributes:
        column (str): The name of the column for which to compute percent deviation.
        new_column (str): The name of the new column to store the percent deviation values.
        by (str): The level by which the average is computed for the deviation calculation.
    """

    def __init__(self, column: str, new_column: str, by: str = "category") -> None:
        super().__init__()
        self._column = column
        self._new_column = new_column
        self._by = by

    @task_logger
    def run(self, data: DataFrame) -> DataFrame:
        """
        Computes the percent deviation of the specified column from the average
        grouped by the 'by' variable and adds it as a new column to the DataFrame.

        Args:
            data (DataFrame): The input PySpark DataFrame containing the data.

        Returns:
            DataFrame: The PySpark DataFrame with the new column containing percent deviation.
        """
        # Calculate the average of the specified column, grouped by the 'by' variable
        avg_values = data.groupBy(self._by).agg(F.avg(self._column).alias("avg_value"))

        # Join the average values back to the original DataFrame
        data = data.join(avg_values, on=self._by, how="left")

        # Compute the percent deviation and add it as a new column
        data = data.withColumn(
            self._new_column,
            ((F.col(self._column) - F.col("avg_value")) / F.col("avg_value")) * 100,
        )

        # Drop the temporary 'avg_value' column
        data = data.drop("avg_value")

        return data
