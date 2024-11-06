#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/data_prep/condition/task.py                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday October 17th 2024 09:19:05 am                                              #
# Modified   : Wednesday November 6th 2024 11:27:20 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Ingest Module"""

import pandas as pd
from pandarallel import pandarallel

from discover.flow.base.task import Task
from discover.infra.service.logging.task import task_logger

# ------------------------------------------------------------------------------------------------ #
pandarallel.initialize(progress_bar=False, nb_workers=12, verbose=0)


# ------------------------------------------------------------------------------------------------ #
class SampleTask(Task):
    """
    A task that samples a fraction of rows from a pandas DataFrame.

    Attributes:
    -----------
    frac : float
        The fraction of the DataFrame to sample, where 0 < frac <= 1.
    random_state : int, optional
        The random seed used to sample the DataFrame, for reproducibility.

    Methods:
    --------
    run(data: pd.DataFrame) -> pd.DataFrame
        Samples the specified fraction of rows from the input DataFrame and returns the sampled DataFrame.
    """

    def __init__(self, frac: float, random_state: int = None) -> None:
        """
        Initializes the SampleTask with a specified sampling fraction and an optional random seed.

        Parameters:
        -----------
        frac : float
            The fraction of the DataFrame to sample, where 0 < frac <= 1.
        random_state : int, optional
            The random seed to use for reproducibility. If None, a random seed will not be set.
        """
        super().__init__()
        self._frac = frac
        self._random_state = random_state

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Samples a fraction of rows from the input DataFrame.

        Parameters:
        -----------
        data : pd.DataFrame
            The DataFrame to sample from.

        Returns:
        --------
        pd.DataFrame
            A new DataFrame containing the sampled rows.
        """
        return data.sample(frac=self._frac, random_state=self._random_state)


# ------------------------------------------------------------------------------------------------ #
#                                    COMPUTE REVIEW AGE                                            #
# ------------------------------------------------------------------------------------------------ #
class ComputeReviewAgeTask(Task):
    """
    A task to compute the "review age" of each entry in a specified date column of a Pandas DataFrame.

    The review age is calculated as the difference in days between the latest date in the column and each
    individual date, providing a measure of how old each review is relative to the most recent one.

    Attributes:
        column (str): The name of the date column to calculate review age from. Defaults to "date".
        new_column (str): Statistic column to create. Default is 'eda_review_age'.

    Methods:
        run(data: DataFrame) -> DataFrame:
            Calculates the review age for each row in the specified date column and returns the DataFrame
            with the new "eda_review_age" column.

    """

    def __init__(
        self, column: str = "date", new_column: str = "eda_review_age"
    ) -> None:
        super().__init__()
        self._column = column
        self._new_column = new_column

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Executes the review age calculation on the specified date column.

        The function first identifies the maximum date within the column and then calculates the number of days
        between each review date and this maximum date, storing the result in a new "eda_review_age" column.

        Args:
            data (pd.DataFrame): The input Pandas DataFrame containing the specified date column.

        Returns:
            DataFrame: The input DataFrame with an additional "eda_review_age" column representing the
            review age in days.
        """
        # Step 1: Find the maximum date in the specified column
        max_date = data[self._column].max()

        # Step 2: Calculate the "review age" as the difference between max_date and each row's date
        data[self._new_column] = (max_date - data[self._column]).days

        return data


# ------------------------------------------------------------------------------------------------ #
#                               COMPUTE AGGREGATE DEVIATION STATS                                  #
# ------------------------------------------------------------------------------------------------ #
class ComputeAggDeviationStats(Task):
    """
    A task to compute the deviation of a specified column's values from the average, grouped by an aggregation key.

    This task calculates the deviation of each value in a specified column from the average of that column within
    groups defined by an aggregation key. This can be used to identify how individual values deviate from the mean
    within a specific context, such as ratings within each app.

    Attributes:
        agg_by (str): The column name to group by for aggregation. Defaults to "app_id".
        column (str): The name of the column for which the deviation is calculated. Defaults to "rating".
        new_column (str): The name of the output column where the deviation will be stored. Defaults to "stats_deviation_rating".

    Methods:
        run(data: DataFrame) -> DataFrame:
            Executes the deviation calculation for the specified column grouped by the aggregation key, and returns
            the DataFrame with the new deviation column.

    Deviation Calculation Steps:
        1. Calculate the average value of the specified column grouped by the aggregation key.
        2. Join the original DataFrame with the grouped averages on the aggregation key.
        3. Calculate the deviation of each value from the group average and store it in the specified column.
    """

    def __init__(
        self,
        agg_by: str = "app_id",
        column: str = "rating",
        new_column: str = "eda_rating_deviation",
    ) -> None:
        """Initializes the ComputeAggDeviationStats task with specified aggregation and target columns."""
        super().__init__()
        self._agg_by = agg_by
        self._column = column
        self._new_column = new_column

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        # Step 1: Calculate average for the aggregation
        app_avg_df = (
            data.groupby(self._agg_by)[self._column]
            .mean()
            .to_frame()
            .rename(columns={self._column: f"eda_{self._column}_avg_by_{self._agg_by}"})
        )
        # Step 2: Join the original DataFrame with the app-level averages
        data = data.join(app_avg_df, on=self._agg_by, how="left")
        # Step 3: Calculate deviations
        data[f"{self._new_column}_by_{self._agg_by}"] = (
            data[self._column] - data[f"eda_{self._column}_avg_by_{self._agg_by}"]
        )

        return data


# ------------------------------------------------------------------------------------------------ #
#                                     REVIEW LENGTH                                                #
# ------------------------------------------------------------------------------------------------ #
class ReviewLengthTask(Task):
    """
    A task to compute the length of text in a specified column by counting the number of words.

    This task adds a new column `eda_review_length` to the input DataFrame, where each value represents
    the number of words in the corresponding text entry from the specified column.

    Attributes:
        _column (str): The name of the column in the DataFrame that contains the text data.
    """

    def __init__(self, column: str) -> None:
        """
        Initializes the ReviewLengthTask with the name of the column to compute review length.

        Args:
            column (str): The name of the column containing the text data to be analyzed.
        """
        super().__init__()
        self._column = column

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Executes the task to compute the word count of text in the specified column.

        This method adds a new column `eda_review_length` to the input DataFrame, containing
        the number of words in each review.

        Args:
            data (pd.DataFrame): The input DataFrame containing the column with text data.

        Returns:
            pd.DataFrame: The DataFrame with an additional column `eda_review_length`.
        """
        data["eda_review_length"] = data[self._column].parallel_apply(
            lambda x: len(str(x).split())
        )
        return data
