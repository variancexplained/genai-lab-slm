#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /discover/infra/operations/read.py                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday May 28th 2024 01:40:18 pm                                                   #
# Modified   : Wednesday September 11th 2024 03:40:31 pm                                           #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""IO Module"""
import pandas as pd

from discover.domain.service.base.task import Task
from discover.domain.value_objects.lifecycle import Stage
from discover.infra.logging.decorator import log_exceptions
from discover.infra.monitor.profiler import task_profiler
from discover.infra.storage.repo.mckinney import McKinneyRepo
from discover.infra.storage.repo.zaharia import ZahariaRepo


# ------------------------------------------------------------------------------------------------ #
#                                       READ PANDAS                                                #
# ------------------------------------------------------------------------------------------------ #
class ReadPandasTask(Task):
    """Read data task

    Args:
        directory (str): Directory containing the data
        filename (str): The name of the file.
        reader (Reader): Reader object.
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

    @log_exceptions()
    @task_profiler
    def run(self) -> pd.DataFrame:
        """Executes the load operation for the pandas DataFrame"""

        self._logger.debug("Executing ReadTask")
        return self._repo.get(stage=self._stage, name=self._name)


# ------------------------------------------------------------------------------------------------ #
#                                       READ SPARK                                                 #
# ------------------------------------------------------------------------------------------------ #
class ReadSparkTask(Task):
    """Read data task

    Args:
        directory (str): Directory containing the data
        filename (str): The name of the file.
        reader (Reader): Reader object.
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

    @log_exceptions()
    @task_profiler
    def run(self) -> pd.DataFrame:
        """Executes the load operation for the pandas DataFrame"""

        self._logger.debug("Executing ReadTask")
        return self._repo.get(stage=self._stage, name=self._name)
