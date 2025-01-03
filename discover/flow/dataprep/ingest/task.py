#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/dataprep/ingest/task.py                                              #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday January 1st 2025 05:54:25 am                                              #
# Modified   : Thursday January 2nd 2025 08:22:12 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""Ingest Task Module"""


import pandas as pd
import pytz
from pandarallel import pandarallel

from discover.flow.base.task import Task
from discover.infra.service.logging.task import task_logger

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
class ConvertDateTimeUTC(Task):
    """
    A task that converts datetime values in a specified column of a DataFrame to UTC.
    It handles both naive and aware timestamps, performing necessary localization
    based on the given timezone, and accounts for Daylight Saving Time (DST) transitions.

    Args:
        column (str): The name of the column containing the datetime values to convert.
                      Defaults to "date".
        timezone (str): The timezone of the input timestamps. Defaults to "America/New_York".
        **kwargs: Additional keyword arguments to pass to the parent class.

    Methods:
        run(data: pd.DataFrame) -> pd.DataFrame:
            Executes the conversion process, applying the UTC conversion to the specified column.

        _convert_to_utc(ts):
            Converts a single timestamp to UTC, handling both naive and aware datetime values.
            Adjusts for non-existent and ambiguous times during DST transitions.
    """

    def __init__(
        self, column: str = "date", timezone: str = "America/New_York", **kwargs
    ) -> None:
        """
        Initializes the ConvertDateTimeUTC task.

        Args:
            column (str): The name of the column containing datetime values to convert. Defaults to "date".
            timezone (str): The timezone of the input timestamps. Defaults to "America/New_York".
            **kwargs: Additional keyword arguments passed to the parent class constructor.
        """
        super().__init__(**kwargs)
        self._column = column
        self._kwargs = kwargs
        self._timezone = timezone

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Applies the UTC conversion to the specified column in the DataFrame.

        Args:
            data (pd.DataFrame): The input DataFrame containing the column to convert.

        Returns:
            pd.DataFrame: The DataFrame with the converted column values in UTC.
        """
        data[self._column] = data[self._column].parallel_apply(self._convert_to_utc)
        return data

    def _convert_to_utc(self, ts):
        """
        Converts a single timestamp to UTC. Handles both naive and aware timestamps,
        and adjusts for non-existent and ambiguous times during Daylight Saving Time (DST) transitions.

        Args:
            ts (pd.Timestamp): The timestamp to convert.

        Returns:
            pd.Timestamp: The converted timestamp in UTC.

        Raises:
            pytz.NonExistentTimeError: Raised when a timestamp does not exist due to DST transition.
            pytz.AmbiguousTimeError: Raised when a timestamp is ambiguous during the end of DST.
        """
        # Check if the timestamp is a datetime
        if isinstance(ts, pd.Timestamp):
            # If tzinfo is not None, the timestamp is aware
            if ts.tzinfo is not None:
                # If it's already timezone-aware, convert directly to UTC
                return ts.astimezone(pytz.utc)
            else:
                # If the timestamp is naive, localize to the specified timezone and then convert to UTC
                local_tz = pytz.timezone(self._timezone)
                try:
                    # Localize the naive timestamp to the timezone and convert to UTC
                    localized_ts = local_tz.localize(ts, is_dst=None)
                    return localized_ts.astimezone(pytz.utc)
                except pytz.NonExistentTimeError:
                    # Handle non-existent time during DST transition (e.g., 2:30 AM in a timezone that skips this time)
                    # Adjust by adding 1 hour and then converting to UTC
                    adjusted_ts = ts + pd.Timedelta(hours=1)
                    localized_ts = local_tz.localize(adjusted_ts, is_dst=None)
                    return localized_ts.astimezone(pytz.utc)
                except pytz.AmbiguousTimeError:
                    # Handle ambiguous time during DST end (e.g., 1:30 AM occurs twice)
                    return local_tz.localize(ts, is_dst=False).astimezone(pytz.utc)
        else:
            # If ts is not a valid timestamp, return it as is
            return ts
