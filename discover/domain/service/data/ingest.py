#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /discover/domain/service/data/ingest.py                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday May 24th 2024 02:47:03 am                                                    #
# Modified   : Wednesday September 18th 2024 02:41:41 pm                                           #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Ingest Task Module"""

import pandas as pd
from dotenv import load_dotenv
from pandarallel import pandarallel

from discover.domain.base.task import Task
from discover.domain.task.core.monitor.announcer import task_announcer
from discover.domain.task.core.monitor.profiler import profiler
from discover.application.service.io.config import ServiceConfig

# ------------------------------------------------------------------------------------------------ #
load_dotenv()
pandarallel.initialize(progress_bar=False, nb_workers=12, verbose=0)


# ------------------------------------------------------------------------------------------------ #
class RemoveNewlinesTask(Task):
    """
    RemoveNewlinesTask is responsible for removing newline characters from the specified text column
    in a DataFrame. It replaces all newline characters (`\n`) with spaces, ensuring the text is in a single line.

    Args:
        config (ServiceConfig): Configuration object that provides:
            - `text_column`: The name of the column containing text where newlines need to be removed.

    Methods:
        run(data: pd.DataFrame) -> pd.DataFrame:
            Executes the task of removing newline characters from the specified text column in the DataFrame.

            Parameters:
            -----------
            data : pd.DataFrame
                The input DataFrame containing the text data.

            Returns:
            --------
            pd.DataFrame:
                The updated DataFrame with newline characters replaced by spaces in the specified text column.
    """

    def __init__(self, config: ServiceConfig) -> None:
        super().__init__(config=config)

    @profiler
    @task_announcer
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Removes newline characters from the specified text column in the DataFrame by replacing them with spaces.

        Parameters:
        -----------
        data : pd.DataFrame
            The input DataFrame that contains the text column with newline characters.

        Returns:
        --------
        pd.DataFrame:
            The updated DataFrame with newlines replaced by spaces in the specified text column.
        """
        data[self._config.text_column] = data[self._config.text_column].str.replace(
            "\n", " "
        )
        return data


# ------------------------------------------------------------------------------------------------ #
class VerifyEncodingTask(Task):
    """
    VerifyEncodingTask is responsible for verifying and re-encoding text data in a specified column
    of a DataFrame to ensure it is properly encoded in UTF-8. It samples the data to check for encoding issues,
    and if issues are found, it re-encodes the entire column to handle potential errors.

    Args:
        config (ServiceConfig): Configuration object that provides:
            - `text_column`: The name of the column containing text to be verified for encoding.
            - `encoding_sample`: The fraction of the dataset to sample for encoding verification.
            - `random_state`: The random seed for reproducibility of the sampling process.

    Methods:
        run(data: pd.DataFrame) -> pd.DataFrame:
            Verifies the UTF-8 encoding of the text data in the specified column. If encoding issues
            are detected in the sample, the entire column is re-encoded to handle these issues.

            Parameters:
            -----------
            data : pd.DataFrame
                The input DataFrame that contains the text data to be verified and potentially re-encoded.

            Returns:
            --------
            pd.DataFrame:
                The updated DataFrame with the text column re-encoded if necessary.
    """

    def __init__(self, config: ServiceConfig) -> None:
        super().__init__(config=config)

    @profiler
    @task_announcer
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Verifies the UTF-8 encoding of the text column in the DataFrame. If encoding issues are found in the sample,
        the entire column is re-encoded to fix the issues.

        Parameters:
        -----------
        data : pd.DataFrame
            The input DataFrame that contains the text data to be checked and potentially re-encoded.

        Returns:
        --------
        pd.DataFrame:
            The updated DataFrame with re-encoded text, if encoding issues were found.

        Raises:
        -------
        UnicodeEncodeError:
            If an encoding issue is detected during sample verification or re-encoding.
        """

        def check_sample_encoding(sample) -> bool:
            """Checks if the sampled data has any UTF-8 encoding issues."""
            try:
                sample.parallel_apply(lambda x: x.encode("utf-8").decode("utf-8"))
                return False  # No encoding issues found
            except UnicodeEncodeError:
                return True  # Encoding issues found

        def re_encode_text(text):
            """Re-encodes a text string to UTF-8, handling encoding issues."""
            try:
                return text.encode("utf-8").decode("utf-8")
            except UnicodeEncodeError:
                self._logger.debug(f"Encoding issue found in text: {text}")
                return text.encode("utf-8", errors="ignore").decode("utf-8")

        sample = data[self._config.text_column].sample(
            frac=self._config.encoding_sample, random_state=self._config.random_state
        )
        if check_sample_encoding(sample=sample):
            self._logger.debug(
                "Encoding issues found in sample. Re-encoding the entire column."
            )
            data[self._config.text_column] = data[
                self._config.text_column
            ].parallel_apply(re_encode_text)
        else:
            self._logger.debug(
                "No encoding issues found in sample. Skipping re-encoding."
            )
        return data


# ------------------------------------------------------------------------------------------------ #
class CastDataTypeTask(Task):
    """
    CastDataTypeTask is responsible for casting the data types of specified columns in a DataFrame
    based on the configuration provided. It iterates through the defined column-datatype mappings
    and ensures that the specified columns in the DataFrame are cast to the desired types.

    Args:
        config (ServiceConfig): Configuration object that contains the datatype mappings.
            The configuration's `datatypes` attribute should provide a dictionary where the keys
            are column names and the values are the target data types.

    Methods:
        run(data: pd.DataFrame) -> pd.DataFrame:
            Executes the task of casting the data types of specified columns in the DataFrame.
            If a column specified in the configuration is not found in the DataFrame, a ValueError
            is raised and an exception is logged.
    """

    def __init__(self, config: ServiceConfig) -> None:
        super().__init__(config=config)

    @profiler
    @task_announcer
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Casts the data types of specified columns in the input DataFrame based on the configuration.

        Parameters:
        -----------
        data : pd.DataFrame
            The DataFrame that contains the data whose columns need to be cast to different data types.

        Returns:
        --------
        pd.DataFrame:
            The updated DataFrame with columns cast to their new data types.

        Raises:
        -------
        ValueError:
            Raised if a column specified in the configuration is not found in the DataFrame.
        """
        for column, dtype in self._config.datatypes.items():
            if column in data.columns:
                data[column] = data[column].astype(dtype)
            else:
                msg = f"Column {column} not found in DataFrame"
                self._logger.exception(msg)
                raise ValueError(msg)
        return data
