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
# Modified   : Thursday November 7th 2024 11:31:05 pm                                              #
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
    and store the result in a new column.

    Attributes:
        column (str): The name of the column for which to compute percent deviation.
        new_column (str): The name of the new column to store the percent deviation values.
    """

    def __init__(self, column: str, new_column: str) -> None:
        """
        Initializes the ComputePercentDeviationTask with the specified column and new column name.

        Args:
            column (str): The column to analyze for percent deviation.
            new_column (str): The column to contain the calculated percent deviation.
        """
        super().__init__()
        self._column = column
        self._new_column = new_column

    @task_logger
    def run(self, data: DataFrame) -> DataFrame:
        """
        Computes the percent deviation of the specified column from the average
        and adds it as a new column to the DataFrame.

        Args:
            data (DataFrame): The input PySpark DataFrame containing the data.

        Returns:
            DataFrame: The PySpark DataFrame with the new column containing percent deviation.
        """
        # Calculate the average of the specified column
        avg_value = data.select(F.avg(self._column)).first()[0]

        # Compute the percent deviation and add it as a new column
        data = data.withColumn(
            self._new_column, ((F.col(self._column) - avg_value) / avg_value) * 100
        )

        return data
