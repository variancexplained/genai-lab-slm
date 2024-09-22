#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/dynamics/stage/base.py                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday September 20th 2024 08:14:05 pm                                              #
# Modified   : Saturday September 21st 2024 11:45:21 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Stage Base Class  Module"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any

from discover.dynamics.base.task import Task
from discover.element.entity.config.service import StageConfig


# ------------------------------------------------------------------------------------------------ #
#                                        STAGE                                                     #
# ------------------------------------------------------------------------------------------------ #
class Stage(ABC):
    """
    Stage serves as an abstract base class stage objects. It manages the configuration,
    task execution, and data flow between source readers, target readers, and target writers. Subclasses are expected
    to implement the `_run_pipeline` method to define specific pipeline behavior.

    Args:
        config (StageConfig): The configuration object that provides source/target readers and writers, as well
            as phase and stage information.

    Methods:
        logger() -> logging.Logger:
            Provides read-only access to the logger instance associated with the pipeline.

        add_task(task: Task):
            Adds a task to the pipeline's sequence of tasks.

        run() -> Any:
            Executes the pipeline, running tasks sequentially or reading from the existing target endpoint if it exists.

        read_endpoint() -> Any:
            Attempts to read the target data from the repository. Returns None if the data doesn't exist or an error occurs.

    Abstract Methods:
        _run_pipeline() -> Any:
            Subclasses must implement this method to define the logic for executing the pipeline tasks. This is where
            the actual processing pipeline is executed.
    """

    def __init__(self, config: StageConfig):
        self._config = config
        self._repo = config.repo

        self._tasks = []
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @property
    def logger(self) -> logging.Logger:
        """
        Provides read-only access to the logger instance for this pipeline.

        Returns:
        --------
        logging.Logger:
            The logger instance associated with this pipeline.
        """
        return self._logger

    def add_task(self, task: Task):
        """
        Adds a task to the pipeline's sequence of tasks.

        Parameters:
        -----------
        task : Task
            A task object to add to the pipeline for execution.
        """
        self._tasks.append(task)
        self._logger.info(
            f"Added {task.name} to the {self.__class__.__name__} pipeline."
        )

    def run(self) -> Any:
        """
        Executes the pipeline tasks in sequence.

        This method first checks if an existing target (endpoint) already exists
        for the pipeline. If it does, it retrieves the data from the endpoint.
        Otherwise, it executes all the tasks in sequence.

        Returns:
        --------
        Any:
            The result of the pipeline execution or the data from the existing endpoint.
        """
        data = None

        if self._config.force:
            data = self._run()
        elif self.endpoint_exists():
            data = self.read_endpoint()
        else:
            data = self._run()

        return data

    def endpoint_exists(self) -> bool:
        """Checks existence of the data endpoint."""
        return self._repo.exists(config=self._config.target_data_config)

    def read_endpoint(self) -> Any:
        """
        Reads and returns the target data from the repository.

        If the target data already exists, it retrieves and returns it.

        Returns:
        --------
        Any:
            The target data from the repository, or None if an error occurs or no data exists.
        """
        data = None
        try:
            data = self._repo.get(config=self._config.target_data_config)
        except Exception as e:
            msg = f"Unable to read endpoint for Stage: {self._config.target_data_config.estage.description} object: {self._config.target_data_config.name}.\n{e}"
            self._logger.exception(msg)
            raise
        return data

    @abstractmethod
    def _run(self) -> Any:
        """
        Abstract method for running the pipeline. Subclasses must implement this method to define the actual
        pipeline execution process.

        Returns:
        --------
        Any:
            The result of the pipeline execution, typically the final output after all tasks have been completed.
        """
        pass
