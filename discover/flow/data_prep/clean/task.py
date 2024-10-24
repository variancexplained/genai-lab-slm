#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/data_prep/clean/task.py                                              #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday October 17th 2024 09:34:20 pm                                              #
# Modified   : Thursday October 24th 2024 12:04:09 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data Cleaning Module"""
import os
import re
import warnings
from abc import abstractmethod
from typing import Any, Optional

import pandas as pd
from pandarallel import pandarallel

from discover.flow.base.task import Task
from discover.infra.service.logging.task import task_logger

# ------------------------------------------------------------------------------------------------ #
warnings.filterwarnings("ignore")
os.environ["PYTHONWARNINGS"] = "ignore"
# ------------------------------------------------------------------------------------------------ #
pandarallel.initialize(nb_workers=12, verbose=False)


# ------------------------------------------------------------------------------------------------ #
#                                    Data Cleaning Task                                            #
# ------------------------------------------------------------------------------------------------ #
class DataCleaningTask(Task):
    """"""

    def __init__(self, dqa_column: str = None, column: Optional[str] = None):
        """
        Initializes the DQATask with the column to inspect and the column to store the results.

        Args:
        -----
        column : str
            The name of the column to inspect for data quality issues.
        dqa_column : str
            The name of the column where the anomalies will be flagged.
        """
        super().__init__()
        self._dqa_column = dqa_column
        self._column = column

    @property
    def column(self) -> str:
        """Returns the name of the column to inspect."""
        return self._column

    @property
    def dqa_column(self) -> str:
        """Returns the name of the column where anomalies are flagged."""
        return self._dqa_column

    @abstractmethod
    def run(self, *args, data: Any, **kwargs) -> Any:
        """
        The core logic of the task. Must be implemented by any subclass.

        Parameters:
        -----------
        *args : tuple
            Positional arguments that the task may require.
        data : Any
            The input data for the task. The specific type of data will depend
            on the implementation of the subclass.
        **kwargs : dict
            Additional keyword arguments that the task may require.

        Returns:
        --------
        Any
            The output of the task, as defined by the subclass implementation.
        """
        pass


# ------------------------------------------------------------------------------------------------ #
#                                  REMOVE ROW TASK                                                 #
# ------------------------------------------------------------------------------------------------ #
class RemoveRowTask(DataCleaningTask):
    """"""

    def __init__(
        self,
        dqa_column: str,
    ) -> None:
        """"""
        super().__init__(dqa_column=dqa_column)

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """"""
        clean_data = data[~self._dqa_column]
        return clean_data


# ------------------------------------------------------------------------------------------------ #
#                                  REPLACE SEQUENCE TASK                                           #
# ------------------------------------------------------------------------------------------------ #
class ReplaceSequenceTask(DataCleaningTask):
    """
    A data cleaning task that replaces substrings matching a regex pattern with a specified replacement value
    in a given column, but only for rows flagged by a data quality anomaly column.

    This task identifies the rows where an anomaly exists (using a boolean DQA column), then applies a regex pattern
    to replace matching substrings with the given replacement string in the target column. The cleaned subset is then
    updated back into the original DataFrame.

    Attributes:
        _pattern (str): The regex pattern to search for in the target column.
        _replacement (str): The string to replace matches of the regex pattern with.
        _dqa_column (str): The column name in the DataFrame that indicates rows with anomalies (boolean column).
        _column (str): The name of the DataFrame column to apply the regex pattern (default is 'content').
    """

    def __init__(
        self, pattern: str, dqa_column: str, replacement: str, column: str = "content"
    ) -> None:
        """
        Initializes the ReplaceSequenceTask.

        Args:
            pattern (str): The regex pattern to search for in the column.
            dqa_column (str): The name of the column that indicates where the anomalies are (boolean values).
            replacement (str): The text to replace the regex pattern with.
            column (str): The column in the DataFrame where the replacement will be applied (default is 'content').
        """
        super().__init__(dqa_column=dqa_column, column=column)
        self._replacement = replacement
        self._pattern = pattern  # This was missing assignment

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Executes the task by replacing the regex pattern with the replacement string in the specified column
        for rows flagged by the data quality column.

        Args:
            data (pd.DataFrame): The input DataFrame containing the target column and the DQA column.

        Returns:
            pd.DataFrame: The updated DataFrame with the regex pattern replaced in the relevant rows.
        """
        # Subset the dataset based on the dqa column
        subset = data.loc[data[self._dqa_column]]

        # Apply the replacement using regex
        subset[self._column] = subset[self._column].parallel_apply(
            lambda x: re.sub(self._pattern, self._replacement, str(x))
        )
        # Update the original dataset with the cleaned subset
        data.update(subset)

        return data
