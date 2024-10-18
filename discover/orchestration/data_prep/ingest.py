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
# Modified   : Thursday October 17th 2024 08:23:30 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
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
