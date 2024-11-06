#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/data_prep/ingest/task.py                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday October 17th 2024 09:19:05 am                                              #
# Modified   : Wednesday November 6th 2024 11:19:38 am                                             #
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
pandarallel.initialize(progress_bar=False, nb_workers=12, verbose=0)


# ------------------------------------------------------------------------------------------------ #
class FilterTask(Task):
    """
    A task that filters a DataFrame based on a date threshold and then samples a fraction of the remaining rows.

    Args:
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
        """
        Initializes the FilterTask with a date threshold and a sampling fraction.

        Args:
            column (str): Column containing the review date.
            date (int): The year to filter the DataFrame by.
            frac (float): The fraction of rows to sample, where 0 < frac <= 1.
            random_state (int, optional): Random seed for reproducibility. Defaults to None.
        """
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
