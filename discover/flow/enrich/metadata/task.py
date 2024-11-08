#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/enrich/metadata/task.py                                              #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 7th 2024 10:15:01 pm                                              #
# Modified   : Thursday November 7th 2024 10:24:53 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Metadata Enrichment Module"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from discover.flow.base.task import Task
from discover.infra.service.logging.task import task_logger


# ------------------------------------------------------------------------------------------------ #
#                                    COMPUTE REVIEW AGE                                            #
# ------------------------------------------------------------------------------------------------ #
class ComputeReviewAgeTask(Task):
    """
    A task to compute the "review age" of each entry in a specified date column of a PySpark DataFrame.

    The review age is calculated as the difference in days between the latest date in the column and each
    individual date, providing a measure of how old each review is relative to the most recent one.

    Attributes:
        column (str): The name of the date column to calculate review age from. Defaults to "date".
        new_column (str): The name of the new column to store the review age. Default is 'enrichment_meta_review_age'.

    Methods:
        run(data: DataFrame) -> DataFrame:
            Calculates the review age for each row in the specified date column and returns the DataFrame
            with the new "enrichment_meta_review_age" column.
    """

    def __init__(
        self, column: str = "date", new_column: str = "enrichment_meta_review_age"
    ) -> None:
        super().__init__()
        self._column = column
        self._new_column = new_column

    @task_logger
    def run(self, data: DataFrame) -> DataFrame:
        """
        Executes the review age calculation on the specified date column.

        The function first identifies the maximum date within the column and then calculates the number of days
        between each review date and this maximum date, storing the result in a new "enrichment_meta_review_age" column.

        Args:
            data (DataFrame): The input PySpark DataFrame containing the specified date column.

        Returns:
            DataFrame: The input DataFrame with an additional "enrichment_meta_review_age" column representing the
            review age in days.
        """
        # Step 1: Find the maximum date in the specified column
        max_date = data.agg(F.max(self._column)).collect()[0][0]

        # Step 2: Calculate the "review age" as the difference between max_date and each row's date
        data = data.withColumn(
            self._new_column, F.datediff(F.lit(max_date), F.col(self._column))
        )

        return data


# ------------------------------------------------------------------------------------------------ #
#                                     REVIEW LENGTH                                                #
# ------------------------------------------------------------------------------------------------ #
class ComputeReviewLengthTask(Task):
    """
    A task to compute the length of text in a specified column by counting the number of words.

    This task adds a new column `enrichment_meta_review_length` to the input DataFrame, where each value represents
    the number of words in the corresponding text entry from the specified column.

    Attributes:
        _column (str): The name of the column in the DataFrame that contains the text data.
        _new_column (str): Name of the column that will contain review length.
    """

    def __init__(
        self, column: str, new_column: str = "enrichment_meta_review_length"
    ) -> None:
        super().__init__()
        self._column = column
        self._new_column = new_column

    @task_logger
    def run(self, data: DataFrame) -> DataFrame:
        """
        Executes the task to compute the word count of text in the specified column.

        This method adds a new column `enrichment_meta_review_length` to the input DataFrame, containing
        the number of words in each review.

        Args:
            data (DataFrame): The input PySpark DataFrame containing the column with text data.

        Returns:
            DataFrame: The DataFrame with an additional column `enrichment_meta_review_length`.
        """
        # Use PySpark's `withColumn` and `size` to compute the word count
        data = data.withColumn(
            self._new_column, F.size(F.split(F.col(self._column), " "))
        )
        return data
