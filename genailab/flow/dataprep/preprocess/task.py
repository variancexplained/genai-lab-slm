#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab                                                                           #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/flow/dataprep/preprocess/task.py                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday January 1st 2025 05:54:25 am                                              #
# Modified   : Saturday February 8th 2025 10:43:03 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""Preprocess Task Module"""


import pandas as pd
from pandarallel import pandarallel

from genailab.flow.base.task import Task
from genailab.infra.service.logging.task import task_logger

# ------------------------------------------------------------------------------------------------ #
pandarallel.initialize(progress_bar=False, nb_workers=18, verbose=0)


# ------------------------------------------------------------------------------------------------ #
class VerifyEncodingTask(Task):
    """
    Task for verifying and correcting UTF-8 encoding issues in a specified text column of a Pandas DataFrame.

    This task ensures that the specified column's text is properly encoded in UTF-8, ignoring and
    removing invalid characters. It is particularly useful for preparing text data for downstream
    processing where consistent encoding is required.

    Args:
        column (str): The name of the text column in the DataFrame to verify and re-encode.

    Methods:
        run(data: pd.DataFrame) -> pd.DataFrame:
            Verifies and re-encodes the specified column in the DataFrame to ensure UTF-8 compliance.
    """

    def __init__(self, column: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self._column = column
        self._kwargs = kwargs

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Verifies UTF-8 encoding of the specified text column and re-encodes it if necessary.

        This method modifies the specified column by encoding it to UTF-8 and decoding it back to
        remove invalid or non-UTF-8 characters, ensuring the column's text complies with UTF-8 standards.

        Args:
            data (pd.DataFrame): The input DataFrame containing the text data.

        Returns:
            pd.DataFrame: The DataFrame with the specified column re-encoded to ensure UTF-8 compliance.
        """
        data[self._column] = (
            data[self._column].str.encode("utf-8", errors="ignore").str.decode("utf-8")
        )
        return data


# ------------------------------------------------------------------------------------------------ #
class CastDataTypeTask(Task):
    """
    Task for casting specified columns in a Pandas DataFrame to desired data types.

    This task casts the specified columns in the DataFrame to the provided data types,
    ensuring that the data conforms to the expected schema. If a column specified in
    the task is not found in the DataFrame, an exception is raised.

    Args:
        datatypes (dict): A dictionary where keys are column names and values are
            the desired data types (e.g., `{"column1": "float", "column2": "int"}`).

    Methods:
        run(data: pd.DataFrame) -> pd.DataFrame:
            Casts the specified columns to the desired data types in the DataFrame.
    """

    def __init__(self, datatypes: dict, **kwargs) -> None:
        super().__init__(**kwargs)
        self._datatypes = datatypes
        self._kwargs = kwargs

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Casts specified columns to the desired data types in the DataFrame.

        This method iterates over the specified columns and casts each one to the
        desired data type. If a column is not found in the DataFrame, it logs an
        error and raises a `ValueError`.

        Args:
            data (pd.DataFrame): The input DataFrame containing the columns to cast.

        Returns:
            pd.DataFrame: The DataFrame with columns cast to the specified data types.

        Raises:
            ValueError: If a specified column is not found in the DataFrame.
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
class RemoveNewlinesTask(Task):
    """
    Task for removing newline characters from a specified text column in a Pandas DataFrame.

    This task replaces all newline characters in the specified text column with spaces,
    ensuring that text data is in a single-line format. It is useful for text preprocessing
    and preparing data for downstream tasks that require consistent formatting.

    Args:
        column (str): The name of the text column in the DataFrame from which to remove newlines.

    Methods:
        run(data: pd.DataFrame, **kwargs) -> pd.DataFrame:
            Removes newline characters from the specified column in the DataFrame.
    """

    def __init__(self, column: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self._column = column

    @task_logger
    def run(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Removes newline characters from the specified column in the DataFrame by replacing
        them with spaces.

        Args:
            data (pd.DataFrame): The input DataFrame containing the text data.
            **kwargs: Additional arguments for compatibility with the task interface.

        Returns:
            pd.DataFrame: The DataFrame with newline characters removed from the specified column.
        """
        data[self._column] = data[self._column].str.replace("\n", " ")
        return data


# ------------------------------------------------------------------------------------------------ #
class ConvertDateTimetoMS(Task):
    """
    A task that converts datetime from nanosecond to  microsecond precision.

    Args:
        column (str): The name of the column containing the datetime values to convert.
                      Defaults to "date".

    Methods:
        run(data: pd.DataFrame) -> pd.DataFrame:
            Executes the conversion process, applying the UTC conversion to the specified column.
    """

    def __init__(self, column: str = "date", **kwargs) -> None:
        super().__init__(**kwargs)
        self._column = column

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Applies the UTC conversion to the specified column in the DataFrame.

        Args:
            data (pd.DataFrame): The input DataFrame containing the column to convert.

        Returns:
            pd.DataFrame: The DataFrame with the converted column values in UTC.
        """

        data[self._column] = self._convert_datetime_ns_to_ms(data[self._column])
        return data

    def _convert_datetime_ns_to_ms(self, datetime_series):
        try:
            # Ensure the series is of datetime type
            if not pd.api.types.is_datetime64_any_dtype(datetime_series):
                raise TypeError("Input series is not of datetime type.")

            # Remove timezone if present
            if datetime_series.dt.tz is not None:
                datetime_series = datetime_series.dt.tz_localize(None)

            # Convert from nanoseconds to milliseconds
            return datetime_series.astype("datetime64[ms]")

        except Exception as e:
            msg = f"Error converting datetime64 from nanosecond to millisecond precision.\n{e}"
            raise Exception(msg)
