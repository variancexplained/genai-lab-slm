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
# Modified   : Saturday September 14th 2024 02:28:14 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from typing import Any

from discover.domain.base.task import Task
from discover.domain.value_objects.config import DataConfig
from discover.domain.value_objects.context import Context
from discover.domain.value_objects.lifecycle import Stage
from discover.infra.monitor.profiler import profiler


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

    @profiler
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


# ------------------------------------------------------------------------------------------------ #
class WriteTask(Task):
    """
    A task class responsible for writing data to a repository. This class extends the base `Task` class
    and is used in a data pipeline to handle writing operations to a repository.

    Attributes:
        data_config (DataConfig): Configuration object containing details about the data repository,
                                  stage, and other necessary configuration parameters.
        context (Context): An object providing the context in which this task is running, including
                           information such as the current stage of the pipeline.
        kwargs (dict): Additional keyword arguments passed during task initialization, allowing flexibility
                       for extra parameters needed during the write operation.

    Methods:
        stage() -> Stage:
            Returns the current pipeline stage.

        run(data: Any) -> Any:
            Executes the task to write data to the configured repository. The `data` argument represents the
            data to be written. The method updates the repository with the provided data and any extra
            configuration provided through the `kwargs`.

            Args:
                data (Any): The data to be written to the repository.

            Returns:
                Any: The same data passed as input, allowing for chained operations or further processing.
    """

    def __init__(self, data_config: DataConfig, context: Context, **kwargs) -> None:
        self._data_config = data_config
        self._context = context
        self._stage = context.stage
        self._kwargs = kwargs

    def stage(self) -> Stage:
        return self._stage

    def run(self, data: Any) -> Any:
        super().run(data=data)
        self._data_config.repo.add(
            stage=self._data_config.stage,
            name=self._data_config.name,
            data=data,
            **self._kwargs,
        )
        return data
