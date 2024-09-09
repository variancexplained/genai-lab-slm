#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appvocai/application/task/data_prep/io.py                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday May 28th 2024 01:40:18 pm                                                   #
# Modified   : Tuesday August 27th 2024 10:54:14 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""IO Module"""
import pandas as pd
from appvocai.application.base import Task
from appvocai.shared.instrumentation.decorator import task_profiler
from appvocai.shared.logging.logging import log_exceptions
from appvocai.utils.base import Converter, Reader, Writer
from appvocai.utils.io import FileReader, FileWriter
from appvocai.utils.repo import ReviewRepo
from appvocai.utils.tempfile import TempFileMgr
from pyspark.sql import DataFrame


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
        reader_cls: type[Reader] = FileReader,
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
    def run_task(self, *args, **kwargs) -> pd.DataFrame:
        """Executes the load operation for the pandas DataFrame"""

        self._logger.debug("Executing ReadTask")
        filepath = self._review_repo.get_filepath(
            directory=self._directory, filename=self._filename
        )
        self._logger.debug(f"Reading from {filepath}")
        results = self._reader.read(filepath=filepath)

        return results


# ------------------------------------------------------------------------------------------------ #
#                                  SAVE PANDAS DATAFRAME                                           #
# ------------------------------------------------------------------------------------------------ #
class WriteTask(Task):
    """Saves a Pandas DataFrame

    Args:
        directory (str): Directory into which the data should be saved.
        filename (str): The filename for the file.
        writer (FileWriter): A Pandas Writer object
    """

    def __init__(
        self,
        directory: str,
        filename: str,
        writer_cls: type[Writer] = FileWriter,
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
    def run_task(self, data: DataFrame) -> None:
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


# ------------------------------------------------------------------------------------------------ #
#                                    CONVERT TASK                                                  #
# ------------------------------------------------------------------------------------------------ #
class ConvertTask(Task):
    """Converts DataFrames to Pandas or Spark

    Args:
        converter_cls (type[ToSpark]): A ToSpark converter class
        tempfile_manager_cls (type[TempFileMgr]): Tempfile manager
        **kwargs: Other keyword arguments.
    """

    def __init__(
        self,
        converter_cls: type[Converter],
        tempfile_manager_cls: type[TempFileMgr] = TempFileMgr,
        **kwargs,
    ) -> None:
        super().__init__()
        self._converter_cls = converter_cls
        self._tempfile_manager_cls = tempfile_manager_cls
        self._kwargs = kwargs

    @task_profiler()
    @log_exceptions()
    def run_task(self, data: pd.DataFrame) -> DataFrame:
        """Converts a Pandas DataFrame to a Spark DataFrame

        Args:
            data (pd.DataFrame): Pandas DataFrame
        """

        converter = self._converter_cls(
            tempfile_manager_cls=self._tempfile_manager_cls, **self._kwargs
        )
        data = converter.convert(data=data)

        return data
