#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/task/clean/noise.py                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday October 17th 2024 09:19:05 am                                              #
# Modified   : Wednesday November 20th 2024 06:49:16 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Ingest Module"""


import pandas as pd
from pandarallel import pandarallel

from discover.flow.data_prep.base.task import DataPrepTask
from discover.infra.service.logging.task import task_logger

# ------------------------------------------------------------------------------------------------ #
pandarallel.initialize(progress_bar=False, nb_workers=18, verbose=0)


# ------------------------------------------------------------------------------------------------ #
class RemoveNewlinesTask(DataPrepTask):
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
