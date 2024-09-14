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
# Modified   : Saturday September 14th 2024 04:04:53 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from typing import Any

from discover.domain.base.task import Task
from discover.domain.service.core.monitor.profiler import profiler
from discover.domain.value_objects.config import DataConfig
from discover.domain.value_objects.context import Context


# ------------------------------------------------------------------------------------------------ #
class ReadTask(Task):
    """
    A task responsible for reading data from a repository as part of a data pipeline.
    This class extends the base `Task` class and retrieves the required data from the repository
    based on the configuration provided.

    Methods:
    --------
    __init__(*args, config: DataConfig, pipeline_context: Context, **kwargs) -> None
        Initializes the `ReadTask` with the given data configuration and pipeline context.

    run(*args, **kwargs) -> Any
        Executes the task to read data from the repository.
        Uses the repository specified in the configuration to retrieve the data based on the stage and name.

        Returns:
        --------
        Any:
            The data retrieved from the repository.
    """

    def __init__(
        self, *args, config: DataConfig, pipeline_context: Context, **kwargs
    ) -> None:
        """
        Initializes the ReadTask with the provided configuration and pipeline context.

        Parameters:
        -----------
        *args :
            Additional positional arguments passed during task initialization.
        config : DataConfig
            Configuration object that provides repository settings and data-related options.
        pipeline_context : Context
            The context object that tracks the execution metadata of the pipeline, such as the current stage.
        **kwargs :
            Additional keyword arguments passed during task initialization.
        """
        super().__init__(config=config, pipeline_context=pipeline_context, **kwargs)

    @profiler
    def run(self, *args, **kwargs) -> Any:
        """
        Executes the task by reading data from the repository based on the configuration.

        Parameters:
        -----------
        *args :
            Positional arguments passed to the task.
        **kwargs :
            Keyword arguments passed to the task.

        Returns:
        --------
        Any:
            The data retrieved from the repository for the given stage and name.
        """
        super().run(args=args, **kwargs)
        return self._config.repo.get(
            stage=self._config.stage, name=self._config.name, **self._kwargs
        )


# ------------------------------------------------------------------------------------------------ #
class WriteTask(Task):
    """
    A task responsible for writing processed data to a repository as part of a data pipeline.
    This class extends the base `Task` class and interacts with a repository to store the output data.

    Methods:
    --------
    __init__(*args, config: DataConfig, pipeline_context: Context, **kwargs) -> None
        Initializes the `WriteTask` with the given data configuration and pipeline context.

    run(data: Any) -> Any
        Executes the task to write the provided data to the repository.
        Uses the repository specified in the configuration to store the data.

        Parameters:
        -----------
        data : Any
            The data to be written to the repository.

        Returns:
        --------
        Any:
            The same data that was written to the repository, for further use in the pipeline.
    """

    def __init__(
        self, *args, config: DataConfig, pipeline_context: Context, **kwargs
    ) -> None:
        """
        Initializes the WriteTask with the provided configuration and pipeline context.

        Parameters:
        -----------
        *args :
            Additional positional arguments passed during task initialization.
        config : DataConfig
            Configuration object that provides repository settings and data-related options.
        pipeline_context : Context
            The context object that tracks the execution metadata of the pipeline, such as the current stage.
        **kwargs :
            Additional keyword arguments passed during task initialization.
        """
        super().__init__(config=config, pipeline_context=pipeline_context, **kwargs)

    @profiler
    def run(self, data: Any) -> Any:
        """
        Executes the task by writing the provided data to the repository.

        Parameters:
        -----------
        data : Any
            The data to be written to the repository.

        Returns:
        --------
        Any:
            The same data that was written to the repository.
        """
        super().run(data=data)
        self._config.repo.add(
            stage=self._config.stage,
            name=self._config.name,
            data=data,
            **self._kwargs,
        )
        return data
