#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /discover/infra/operations/file_task/write.py                                       #
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
from pyspark.sql import DataFrame

from discover.domain.service.base.task import Task
from discover.infra.fileio.base import Writer
from discover.infra.fileio.pandas.writer import PandasWriter
from discover.infra.logging.decorator import log_exceptions
from discover.infra.monitor.profiler import task_profiler
from discover.infra.repo.review import ReviewRepo


# ------------------------------------------------------------------------------------------------ #
#                                  SAVE PANDAS DATAFRAME                                           #
# ------------------------------------------------------------------------------------------------ #
class WriteTask(Task):
    """Saves a Pandas DataFrame

    Args:
        directory (str): Directory into which the data should be saved.
        filename (str): The filename for the file.
        writer (PandasWriter): A Pandas Writer object
    """

    def __init__(
        self,
        directory: str,
        filename: str,
        writer_cls: type[Writer] = PandasWriter,
        review_repo_cls: type[ReviewRepo] = ReviewRepo,
        **kwargs,
    ) -> None:
        super().__init__()
        self._writer = writer_cls(**kwargs)
        self._directory = directory
        self._filename = filename
        self._review_repo = review_repo_cls()
        self._kwargs = kwargs

    @task_profiler()
    @log_exceptions()
    def run(self, data: DataFrame) -> None:
        """Saves the dataframe to file.

        Args:
            data (DataFrame): Pandas DataFrame
        """
        self._logger.debug("Executing WriteTask")

        filepath = self._review_repo.get_filepath(
            directory=self._directory, filename=self._filename
        )
        self._logger.debug(f"Writing to {filepath}")
        try:
            self._writer.write(data=data, filepath=filepath)
        except Exception as e:
            msg = f"Exception occurred writing data to {filepath}.\n{e}"
            self.logger.exception(msg)
            raise
