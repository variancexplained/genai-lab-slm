#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/orchestration/data_prep/ingest.py                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday October 17th 2024 09:19:05 am                                              #
# Modified   : Thursday October 17th 2024 11:38:38 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from datetime import datetime

import pandas as pd

from discover.infra.service.logging.task import task_logger
from discover.orchestration.base.task import Task


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
class FilterTask(Task):
    """
    A task that filters a DataFrame based on a date threshold and then samples a fraction of the remaining rows.

    Args:
        date_column (str): Column containing the review date.
        date (int): The year to filter the DataFrame by. Only rows with dates after this year will be included.
        frac (float): The fraction of the DataFrame to sample, where 0 < frac <= 1.
        random_state (int, optional): Random seed for reproducibility of the sample. Defaults to None.

    Attributes:
        _date (datetime): The date threshold for filtering the DataFrame.
        _frac (float): The fraction of rows to sample from the filtered DataFrame.
        _random_state (int): The random seed used for sampling.
    """

    def __init__(
        self, date_column: str, date: int, frac: float, random_state: int = None
    ) -> None:
        """
        Initializes the FilterTask with a date threshold and a sampling fraction.

        Args:
            date (int): The year to filter the DataFrame by.
            frac (float): The fraction of rows to sample, where 0 < frac <= 1.
            random_state (int, optional): Random seed for reproducibility. Defaults to None.
        """
        super().__init__()
        self._date_column = date_column
        self._date = datetime(date, 1, 1)
        self._frac = frac
        self._random_state = random_state

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Filters the DataFrame based on the date threshold and then samples a fraction of the remaining rows.

        Args:
            data (pd.DataFrame): The input DataFrame containing a "date" column.

        Returns:
            pd.DataFrame: A new DataFrame containing the sampled rows after filtering by the date threshold.
        """
        df = data.loc[data[self._date_column] > self._date]
        return df.sample(frac=self._frac, random_state=self._random_state)
