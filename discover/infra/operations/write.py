#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /discover/infra/operations/write.py                                                 #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday May 28th 2024 01:40:18 pm                                                   #
# Modified   : Wednesday September 11th 2024 03:24:04 pm                                           #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""IO Module"""
import pandas as pd
from pyspark.sql import DataFrame

from discover.domain.service.base.task import Task
from discover.domain.value_objects.lifecycle import Stage
from discover.infra.logging.decorator import log_exceptions
from discover.infra.monitor.profiler import task_profiler
from discover.infra.storage.repo.mckinney import McKinneyRepo
from discover.infra.storage.repo.zaharia import ZahariaRepo


# ------------------------------------------------------------------------------------------------ #
#                                 WRITE PANDAS DATAFRAME                                           #
# ------------------------------------------------------------------------------------------------ #
class WritePandasTask(Task):
    """Saves a Pandas DataFrame

    Args:
        directory (str): Directory into which the data should be saved.
        filename (str): The filename for the file.
        writer (PandasWriter): A Pandas Writer object
    """

    __STAGE = Stage.MULTI

    def __init__(
        self,
        stage: Stage,
        name: str,
        repo_cls: type[McKinneyRepo] = McKinneyRepo,
        **kwargs,
    ) -> None:
        super().__init__(stage=self.__STAGE)
        self._stage = stage
        self._name = name
        self._repo = repo_cls()
        self._kwargs = kwargs

    @task_profiler
    @log_exceptions()
    def run(self, data: pd.DataFrame) -> None:
        """Saves the dataframe to file.

        Args:
            data (DataFrame): Pandas DataFrame
        """
        self._logger.debug("Executing WriteTask")
        self._repo.add(data=data, stage=self._stage, name=self._name)


# ------------------------------------------------------------------------------------------------ #
#                                 WRITE SPARK DATAFRAME                                           #
# ------------------------------------------------------------------------------------------------ #
class WriteSparkTask(Task):
    """Saves a Pandas DataFrame

    Args:
        directory (str): Directory into which the data should be saved.
        filename (str): The filename for the file.
        writer (PandasWriter): A Pandas Writer object
    """

    __STAGE = Stage.MULTI

    def __init__(
        self,
        stage: Stage,
        name: str,
        repo_cls: type[ZahariaRepo] = ZahariaRepo,
        **kwargs,
    ) -> None:
        super().__init__(stage=self.__STAGE)
        self._stage = stage
        self._name = name
        self._repo = repo_cls()
        self._kwargs = kwargs

    @task_profiler
    @log_exceptions()
    def run(self, data: DataFrame) -> None:
        """Saves the dataframe to file.

        Args:
            data (DataFrame): Pandas DataFrame
        """
        self._logger.debug("Executing WriteTask")
        self._repo.add(data=data, stage=self._stage, name=self._name)
