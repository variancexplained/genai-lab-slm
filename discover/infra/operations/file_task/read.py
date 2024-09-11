#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /discover/infra/operations/file_task/read.py                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday May 28th 2024 01:40:18 pm                                                   #
# Modified   : Wednesday September 11th 2024 11:08:29 am                                           #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""IO Module"""
import pandas as pd

from discover.domain.service.base.task import Task
from discover.infra.fileio.base import Reader
from discover.infra.fileio.pandas.reader import PandasReader
from discover.infra.logging.decorator import log_exceptions
from discover.infra.monitor.profiler import task_profiler
from discover.infra.repo.review import ReviewRepo


# ------------------------------------------------------------------------------------------------ #
#                                       LOAD PANDAS                                                #
# ------------------------------------------------------------------------------------------------ #
class ReadTask(Task):
    """Read data task

    Args:
        directory (str): Directory containing the data
        filename (str): The name of the file.
        reader (Reader): Reader object.
    """

    def __init__(
        self,
        directory: str,
        filename: str,
        reader_cls: type[Reader] = PandasReader,
        review_repo_cls: type[ReviewRepo] = ReviewRepo,
        **kwargs,
    ) -> None:
        super().__init__()
        self._reader = reader_cls(**kwargs)
        self._review_repo = review_repo_cls()
        self._directory = directory
        self._filename = filename
        self._kwargs = kwargs

    @log_exceptions()
    @task_profiler()
    def run(self, *args, **kwargs) -> pd.DataFrame:
        """Executes the load operation for the pandas DataFrame"""

        self._logger.debug("Executing ReadTask")
        filepath = self._review_repo.get_filepath(
            directory=self._directory, filename=self._filename
        )
        self._logger.debug(f"Reading from {filepath}")
        results = self._reader.read(filepath=filepath)

        return results
