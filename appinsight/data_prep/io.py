#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/data_prep/io.py                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday May 28th 2024 01:40:18 pm                                                   #
# Modified   : Thursday June 27th 2024 04:53:21 am                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""IO Module"""
import pandas as pd
from pyspark.sql import DataFrame

from appinsight.infrastructure.logging import log_exceptions
from appinsight.infrastructure.profiling.decorator import task_profiler
from appinsight.utils.base import Converter, Reader, Writer
from appinsight.utils.io import PandasReader, PandasWriter
from appinsight.utils.repo import DatasetRepo
from appinsight.utils.tempfile import TempFileMgr
from appinsight.workflow.task import Task


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
        dsm_cls: type[DatasetRepo] = DatasetRepo,
        **reader_kwargs,
    ) -> None:
        super().__init__()
        self._reader = reader_cls(**reader_kwargs)
        self._dsm = dsm_cls()
        self._directory = directory
        self._filename = filename

    @log_exceptions()
    @task_profiler()
    def execute_task(self, *args, **kwargs) -> pd.DataFrame:
        """Executes the load operation for the pandas DataFrame"""

        self._logger.debug("Executing ReadTask")
        filepath = self._dsm.get_filepath(
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
        writer (PandasWriter): A Pandas Writer object
    """

    def __init__(
        self,
        directory: str,
        filename: str,
        writer_cls: type[Writer] = PandasWriter,
        dsm_cls: type[DatasetRepo] = DatasetRepo,
        **writer_kwargs,
    ) -> None:
        super().__init__()
        self._writer = writer_cls(**writer_kwargs)
        self._directory = directory
        self._filename = filename
        self._dsm = dsm_cls()

    @task_profiler()
    @log_exceptions()
    def execute_task(self, data: DataFrame, *args, **kwargs) -> None:
        """Saves the dataframe to file.

        Args:
            data (DataFrame): Pandas DataFrame
        """
        self._logger.debug("Executing WriteTask")

        filepath = self._dsm.get_filepath(
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
    def execute_task(self, data: pd.DataFrame) -> DataFrame:
        """Converts a Pandas DataFrame to a Spark DataFrame

        Args:
            data (pd.DataFrame): Pandas DataFrame
        """

        converter = self._converter_cls(
            tempfile_manager_cls=self._tempfile_manager_cls, **self._kwargs
        )
        data = converter.convert(data=data)

        return data
