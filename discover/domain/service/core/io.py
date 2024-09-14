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
# Created    : Friday September 13th 2024 07:13:32 pm                                              #
# Modified   : Saturday September 14th 2024 05:25:00 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from typing import Any

from discover.domain.base.task import Task
from discover.domain.value_objects.config import DataConfig
from discover.domain.value_objects.context import Context
from discover.domain.value_objects.lifecycle import Stage


# ------------------------------------------------------------------------------------------------ #
class ReadTask(Task):
    """
    Task for reading data from a repository.

    This task is responsible for fetching data from a repository using the provided
    `DataConfig`. It executes as part of a pipeline or service to retrieve the required
    input data.

    Attributes:
    -----------
    _data_config : DataConfig
        Configuration object containing the repository details, stage, and name for the data.
    _context : Context
        Context object that tracks metadata such as service type, name, and stage during task execution.
    _stage : Stage
        The current stage of the task, derived from the context.

    Methods:
    --------
    stage() -> Stage:
        Returns the stage of the task, which is set from the context.

    run(*args, **kwargs) -> Any:
        Executes the task, retrieves data from the repository, and returns it.
    """

    def __init__(self, data_config: DataConfig, context: Context) -> None:
        """
        Initializes the ReadTask with the given data configuration and context.

        Parameters:
        -----------
        data_config : DataConfig
            Configuration object containing the details of the repository, stage, and name for the data.
        context : Context
            Context object used to track execution metadata, including the stage and other task-specific information.
        """
        self._data_config = data_config
        self._context = context
        self._stage = context.stage

    def stage(self) -> Stage:
        """
        Returns the stage of the task, which is set from the context.

        Returns:
        --------
        Stage:
            The current stage of the task.
        """
        return self._stage

    def run(self, *args, **kwargs) -> Any:
        """
        Executes the task by retrieving data from the repository based on the stage and name
        defined in the `DataConfig`.

        Parameters:
        -----------
        *args : Any
            Positional arguments passed during the task's execution.
        **kwargs : Any
            Keyword arguments passed during the task's execution.

        Returns:
        --------
        Any:
            The retrieved data from the repository.
        """
        super().run(args=args, **kwargs)
        return self._data_config.repo.get(
            stage=self._data_config.stage, name=self._data_config.name
        )
