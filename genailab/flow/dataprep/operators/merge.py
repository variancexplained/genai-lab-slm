#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/flow/dataprep/operators/merge.py                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday February 4th 2025 01:58:45 am                                               #
# Modified   : Tuesday February 4th 2025 03:15:54 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""Merge Task Module"""
import logging
from typing import Type

import pandas as pd

from genailab.flow.base.task import Task
from genailab.infra.service.logging.task import task_logger
from genailab.infra.utils.file.io import IOService


# ------------------------------------------------------------------------------------------------ #
class MergeTask(Task):
    """
    A task that merges an input DataFrame with data read from a file.

    This task reads data from a specified file using the provided `IOService` class (default is `IOService`).
    It then merges this data with the input DataFrame based on the "id" column and returns the resulting DataFrame.

    Args:
        filepath (str): The path to the file containing data to merge with the input DataFrame.
        io_cls (Type[IOService], optional): The class used for reading the file. Defaults to `IOService`.

    Methods:
        run(data: pd.DataFrame) -> pd.DataFrame:
            Merges the input DataFrame with data read from the file on the "id" column.
    """

    def __init__(self, filepath: str, io_cls: Type[IOService] = IOService):
        super().__init__()
        self._filepath = filepath
        self._io = io_cls()

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Merges the input DataFrame with data read from the file on the "id" column.

        The method reads data from the file specified in `self._filepath` and performs a merge
        with the provided `data` DataFrame on the "id" column, then returns the merged DataFrame.

        Args:
            data (pd.DataFrame): The input DataFrame to be merged.

        Returns:
            pd.DataFrame: The resulting DataFrame after the merge.
        """
        sa = self._io.read(filepath=self._filepath)
        # Subset, we only need id and sentiment
        sa = sa[["id", "sentiment"]]
        # Convert id to string
        sa["id"] = sa["id"].astype(str)

        # Validate the datasets
        if sa.shape[0] != data.shape[0]:
            msg = f"Mismatchedd shape in MergeTask. Data length: {data.shape[0]}, SA Length {sa.shape[0]}"
            logging.error(msg)
            raise ValueError(msg)

        if "id" not in data.columns or "id" not in sa.columns:
            msg = f"One or both datasets do not have an 'id' column."
            logging.error(msg)
            raise ValueError(msg)

        data = data.merge(right=sa, on="id", how="left")
        return data



