#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/domain/service/core/io.py                                                 #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday September 13th 2024 12:47:03 pm                                              #
# Modified   : Friday September 13th 2024 01:06:55 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import pandas as pd

from discover.domain.service.base.repo import Repo
from discover.domain.service.base.task import Task
from discover.domain.value_objects.lifecycle import Stage
from discover.infra.logging.decorator import log_exceptions
from discover.infra.monitor.profiler import task_profiler


# ------------------------------------------------------------------------------------------------ #
#                                      READ TASK                                                   #
# ------------------------------------------------------------------------------------------------ #
class ReadTask(Task):
    """
    A task that reads data from persistent storage.

    This task is responsible for loading a dataset (Pandas DataFrame) from a repository
    using the provided repository class (`repo_cls`). It encapsulates the logic to retrieve
    data for a specific pipeline stage, with an option to specify the name for the data being loaded.

    Attributes:
        __STAGE (Stage): The core stage of the pipeline this task belongs to.
        _name (str): The name used to identify the data being loaded (default: "reviews").
        _repo (Repo): An instance of the repository class responsible for loading the data.
        _kwargs (dict): Additional keyword arguments passed to the repository.

    Args:
        repo_cls (type[Repo]): The repository class used to handle data loading.
        stage (Stage): The stage of the pipeline where the task is executed.
        name (str, optional): The name used to identify the data being loaded (default: "reviews").
        **kwargs: Additional arguments for the repository.
    """

    __STAGE = Stage.CORE

    def __init__(
        self,
        repo_cls: type[Repo],
        stage: Stage,
        name: str = "reviews",
        **kwargs,
    ) -> None:
        """Initializes the ReadTask with the given repository, stage, and name.

        Args:
            repo_cls (type[Repo]): The repository class responsible for loading the data.
            stage (Stage): The pipeline stage where this task is performed.
            name (str, optional): The name used to identify the loaded data (default is "reviews").
            **kwargs: Additional keyword arguments for the repository.
        """
        super().__init__(stage=stage)
        self._name = name
        self._repo = repo_cls()
        self._kwargs = kwargs

    @log_exceptions()
    @task_profiler
    def run(self) -> pd.DataFrame:
        """Executes the load operation for the pandas DataFrame.

        This method retrieves the data (Pandas DataFrame) from the repository's `get` method,
        using the specified stage and name. Logs the process and handles exceptions.

        Returns:
            pd.DataFrame: The data loaded from the repository.
        """
        self._logger.debug("Executing ReadTask")
        return self._repo.get(stage=self._stage, name=self._name)


# ------------------------------------------------------------------------------------------------ #
#                                      WRITE TASK                                                  #
# ------------------------------------------------------------------------------------------------ #
class WriteTask(Task):
    """
    A task that writes data to a persistent storage.

    This task takes a dataset (Pandas DataFrame) and persists it using the provided repository class (`repo_cls`).
    It encapsulates the logic to handle persistence at a specific pipeline stage, and it can be configured
    with a custom name for identifying the data.

    Attributes:
        __STAGE (Stage): The core stage of the pipeline this task belongs to.
        _name (str): The name used to identify the data being persisted (default: "reviews").
        _repo (Repo): An instance of the repository class responsible for persisting the data.
        _kwargs (dict): Additional keyword arguments passed to the repository.

    Args:
        repo_cls (type[Repo]): The repository class used to handle data persistence.
        stage (Stage): The stage of the pipeline where the task is executed.
        name (str, optional): The name used to identify the persisted data (default: "reviews").
        **kwargs: Additional arguments for the repository.
    """

    __STAGE = Stage.CORE

    def __init__(
        self,
        repo_cls: type[Repo],
        stage: Stage,
        name: str = "reviews",
        **kwargs,
    ) -> None:
        """Initializes the WriteTask with the given repository, stage, and name.

        Args:
            repo_cls (type[Repo]): The repository class responsible for data persistence.
            stage (Stage): The pipeline stage where this task is performed.
            name (str, optional): The name used to identify the persisted data (default is "reviews").
            **kwargs: Additional keyword arguments for the repository.
        """
        super().__init__(stage=stage)
        self._name = name
        self._repo = repo_cls()
        self._kwargs = kwargs

    @task_profiler
    @log_exceptions()
    def run(self, data: pd.DataFrame) -> None:
        """Saves the dataframe to a repository.

        This method executes the task by saving the provided Pandas DataFrame
        using the repository's `add` method. Logs the process and handles exceptions.

        Args:
            data (pd.DataFrame): The data to be persisted.
        """
        self._logger.debug("Executing WriteTask")
        self._repo.add(data=data, stage=self._stage, name=self._name)
