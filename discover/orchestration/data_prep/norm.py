#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/orchestration/data_prep/norm.py                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday September 18th 2024 08:39:32 pm                                           #
# Modified   : Thursday October 17th 2024 08:21:47 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Ingest Task Module"""


import pandas as pd
from dotenv import load_dotenv
from pandarallel import pandarallel

from discover.infra.service.logging.task import task_logger
from discover.orchestration.base.task import Task

# ------------------------------------------------------------------------------------------------ #
load_dotenv()
pandarallel.initialize(progress_bar=False, nb_workers=12, verbose=0)


# ------------------------------------------------------------------------------------------------ #
class RemoveNewlinesTask(Task):
    """
    A task that removes newlines from a specified text column in a pandas DataFrame.

    Args:
        text_column (str): The name of the column in the DataFrame that contains text data.

    Attributes:
        _text_column (str): The name of the column in the DataFrame that contains text data from which newlines will be removed.

    Methods:
        run(data: pd.DataFrame, **kwargs) -> pd.DataFrame: Removes newlines from the specified text column in the input DataFrame.
    """

    def __init__(self, text_column: str) -> None:
        """
        Initializes the RemoveNewlinesTask with the specified column name.

        Args:
            text_column (str): The name of the column in the DataFrame that contains text data.
        """
        super().__init__()
        self._text_column = text_column

    @task_logger
    def run(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Removes newlines from the specified text column in the provided DataFrame.

        Args:
            data (pd.DataFrame): The input DataFrame containing the text data.
            **kwargs: Additional keyword arguments (not used in this implementation).

        Returns:
            pd.DataFrame: A DataFrame with newlines removed from the specified text column.
        """
        data[self._text_column] = data[self._text_column].str.replace("\n", " ")
        return data


# ------------------------------------------------------------------------------------------------ #
class VerifyEncodingTask(Task):
    """
    A task that verifies and fixes UTF-8 encoding issues in a specified text column of a pandas DataFrame.

    Args:
        text_column (str): The name of the column in the DataFrame that contains text data.
        encoding_sample (float): The fraction of rows to sample for checking encoding issues, where 0 < encoding_sample <= 1.
        random_state (int, optional): Random seed for reproducibility of the sample. Defaults to None.

    Attributes:
        _text_column (str): The column name in the DataFrame to check for encoding issues.
        _encoding_sample (float): The fraction of data to sample for encoding verification.
        _random_state (int): The random seed for sampling.

    Methods:
        run(data: pd.DataFrame) -> pd.DataFrame: Verifies and fixes any UTF-8 encoding issues in the specified text column.
    """

    def __init__(
        self, text_column: str, encoding_sample: float, random_state: int = None
    ) -> None:
        """
        Initializes the VerifyEncodingTask with the text column and sample fraction for encoding verification.

        Args:
            text_column (str): The column in the DataFrame that contains text data.
            encoding_sample (float): The fraction of rows to sample for checking encoding issues.
            random_state (int, optional): Random seed for sampling. Defaults to None.
        """
        super().__init__()
        self._text_column = text_column
        self._encoding_sample = encoding_sample
        self._random_state = random_state

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Verifies the UTF-8 encoding of a sample of the text column and re-encodes the entire column if issues are found.

        Args:
            data (pd.DataFrame): The input DataFrame containing the text data.

        Returns:
            pd.DataFrame: The DataFrame with UTF-8 encoding issues resolved in the specified text column.
        """

        def check_sample_encoding(sample) -> bool:
            """
            Checks if the sampled data has any UTF-8 encoding issues.

            Args:
                sample (pd.Series): A sample of the text data.

            Returns:
                bool: True if encoding issues are found, False otherwise.
            """
            try:
                sample.parallel_apply(lambda x: x.encode("utf-8").decode("utf-8"))
                return False  # No encoding issues found
            except UnicodeEncodeError:
                return True  # Encoding issues found

        def re_encode_text(text):
            """
            Re-encodes a text string to UTF-8, handling any encoding errors.

            Args:
                text (str): The input text to re-encode.

            Returns:
                str: The re-encoded text string.
            """
            try:
                return text.encode("utf-8").decode("utf-8")
            except UnicodeEncodeError:
                self._logger.debug(f"Encoding issue found in text: {text}")
                return text.encode("utf-8", errors="ignore").decode("utf-8")

        sample = data[self._text_column].sample(
            frac=self._encoding_sample, random_state=self._random_state
        )
        if check_sample_encoding(sample=sample):
            self._logger.debug(
                "Encoding issues found in sample. Re-encoding the entire column."
            )
            data[self._text_column] = data[self._text_column].parallel_apply(
                re_encode_text
            )
        else:
            self._logger.debug(
                "No encoding issues found in sample. Skipping re-encoding."
            )
        return data


# ------------------------------------------------------------------------------------------------ #
class CastDataTypeTask(Task):
    """
    A task that casts the data types of specified columns in a pandas DataFrame.

    Args:
        datatypes (dict): A dictionary where the keys are column names and the values are the desired data types for each column.

    Attributes:
        _datatypes (dict): The dictionary that maps column names to the desired data types.

    Methods:
        run(data: pd.DataFrame) -> pd.DataFrame: Casts the specified columns to the desired data types.
    """

    def __init__(self, datatypes: dict) -> None:
        """
        Initializes the CastDataTypeTask with a dictionary of column names and their corresponding data types.

        Args:
            datatypes (dict): A dictionary where keys are column names and values are the target data types (e.g., 'float', 'int', 'str').
        """
        super().__init__()
        self._datatypes = datatypes

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Casts the data types of the specified columns in the DataFrame.

        Args:
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
