#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/data_processing/data_prep/ingest/task.py                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday October 17th 2024 09:19:05 am                                              #
# Modified   : Sunday November 17th 2024 01:04:04 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Ingest Module"""

from datetime import datetime
from typing import Optional

import pandas as pd
from pandarallel import pandarallel

from discover.flow.base.task import Task
from discover.infra.service.logging.task import task_logger

# ------------------------------------------------------------------------------------------------ #
pandarallel.initialize(progress_bar=False, nb_workers=18, verbose=0)


# ------------------------------------------------------------------------------------------------ #
class FilterTask(Task):
    """
    A task that filters a DataFrame based on a date threshold and then samples a fraction of the remaining rows.

    Args:
        stage_id (str): Id for the stage to which the task belongs.
        column (str): Column containing the review date.
        date (int): The year to filter the DataFrame by. Only rows with dates after this year will be included.
        frac (float): The fraction of the DataFrame to sample, where 0 < frac <= 1.
        random_state (int, optional): Random seed for reproducibility of the sample. Defaults to None.

    """

    def __init__(
        self,
        column: str,
        frac: float = 1.0,
        date: Optional[int] = None,
        random_state: int = None,
    ) -> None:
        super().__init__()
        self._column = column
        self._date = date
        self._frac = frac
        self._random_state = random_state

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Filters the DataFrame based on the date threshold and then samples a fraction of the remaining rows.

        Args:
        stage_id (str): Id for the stage to which the task belongs.
            data (pd.DataFrame): The input DataFrame containing a "date" column.

        Returns:
            pd.DataFrame: A new DataFrame containing the sampled rows after filtering by the date threshold.
        """
        df = data.copy()
        if self._date:
            self._date = datetime(self._date, 1, 1)
            df = data.loc[data[self._column] > self._date]
        if self._frac < 1.0:
            df = df.sample(frac=self._frac, random_state=self._random_state)
        return df


# ------------------------------------------------------------------------------------------------ #
class VerifyEncodingTask(Task):
    """
    A task that verifies and fixes UTF-8 encoding issues in a specified text column of a pandas DataFrame.

    Args:
        stage_id (str): Id for the stage to which the task belongs.
        column (str): The name of the column in the DataFrame that contains text data.
        encoding_sample (float): The fraction of rows to sample for checking encoding issues, where 0 < encoding_sample <= 1.
        random_state (int, optional): Random seed for reproducibility of the sample. Defaults to None.

    Attributes:
        _column (str): The column name in the DataFrame to check for encoding issues.
        _encoding_sample (float): The fraction of data to sample for encoding verification.
        _random_state (int): The random seed for sampling.

    Methods:
        run(data: pd.DataFrame) -> pd.DataFrame: Verifies and fixes any UTF-8 encoding issues in the specified text column.
    """

    def __init__(self, column: str) -> None:
        super().__init__()
        self._column = column

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Verifies the UTF-8 encoding of a sample of the text column and re-encodes the entire column if issues are found.

        Args:
        stage_id (str): Id for the stage to which the task belongs.
            data (pd.DataFrame): The input DataFrame containing the text data.

        Returns:
            pd.DataFrame: The DataFrame with UTF-8 encoding issues resolved in the specified text column.
        """

        data[self._column] = (
            data[self._column].str.encode("utf-8", errors="ignore").str.decode("utf-8")
        )
        return data


# ------------------------------------------------------------------------------------------------ #
class RemoveNewlinesTask(Task):
    """
    A task that removes newlines from a specified text column in a pandas DataFrame.

    Args:
        stage_id (str): Id for the stage to which the task belongs.
        column (str): The name of the column in the DataFrame that contains text data.

    Attributes:
        _column (str): The name of the column in the DataFrame that contains text data from which newlines will be removed.

    Methods:
        run(data: pd.DataFrame, **kwargs) -> pd.DataFrame: Removes newlines from the specified text column in the input DataFrame.
    """

    def __init__(self, column: str) -> None:
        super().__init__()
        self._column = column

    @task_logger
    def run(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Removes newlines from the specified text column in the provided DataFrame.

        Args:
        stage_id (str): Id for the stage to which the task belongs.
            data (pd.DataFrame): The input DataFrame containing the text data.
            **kwargs: Additional keyword arguments (not used in this implementation).

        Returns:
            pd.DataFrame: A DataFrame with newlines removed from the specified text column.
        """

        data[self._column] = data[self._column].str.replace("\n", " ")
        return data


# ------------------------------------------------------------------------------------------------ #
class CastDataTypeTask(Task):
    """
    A task that casts the data types of specified columns in a pandas DataFrame.

    Args:
        stage_id (str): Id for the stage to which the task belongs.
        datatypes (dict): A dictionary where the keys are column names and the values are the desired data types for each column.

    Attributes:
        _datatypes (dict): The dictionary that maps column names to the desired data types.

    Methods:
        run(data: pd.DataFrame) -> pd.DataFrame: Casts the specified columns to the desired data types.
    """

    def __init__(self, datatypes: dict) -> None:
        super().__init__()
        self._datatypes = datatypes

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Casts the data types of the specified columns in the DataFrame.

        Args:
        stage_id (str): Id for the stage to which the task belongs.
            data (pd.DataFrame): The input DataFrame in which columns will be cast to new data types.

        Returns:
            pd.DataFrame: The DataFrame with columns cast to the specified data types.

        Raises:
            ValueError: If a column specified in the datatypes dictionary is not found in the DataFrame.
        """

        for column, dtype in self._datatypes.items():
            if column in data.columns:
                data[column] = data[column].astype(dtype)
            else:
                msg = f"Column {column} not found in DataFrame"
                self._logger.exception(msg)
                raise ValueError(msg)

        return data


# ------------------------------------------------------------------------------------------------ #
class AddReviewLengthTask(Task):
    """
    A task for adding a new column to a pandas DataFrame that contains the length of the text
    in a specified column.

    This class calculates the length of each text entry in the given column and stores
    the result in a new column. The operation is performed in a vectorized manner for
    efficiency.

    Attributes:
        column (str): The name of the column containing the text for which the length
            will be calculated. Defaults to 'content'.
        new_column (str): The name of the new column where the lengths will be stored.
            Defaults to 'review_length'.
    """

    def __init__(
        self, column: str = "content", new_column: str = "review_length"
    ) -> None:
        super().__init__()
        self._column = column
        self._new_column = new_column

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Adds a new column to the DataFrame with the length of the text in the specified column.

        Args:
        stage_id (str): Id for the stage to which the task belongs.
            data (pd.DataFrame): The input pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: The modified DataFrame with the new column containing the lengths of the text.
        """
        data[self._new_column] = data[self._column].parallel_apply(
            lambda x: len(x.split())
        )
        return data
