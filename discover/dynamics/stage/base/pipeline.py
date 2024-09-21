#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /discover/dynamics/stage/base/pipeline.py                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday June 30th 2024 03:42:28 am                                                   #
# Modified   : Friday September 20th 2024 08:17:23 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any

from discover.application.ops.announcer import task_announcer
from discover.application.ops.cachenow import cachenow
from discover.application.ops.profiler import profiler
from discover.substance.entity.config.service import StageConfig
from discover.substance.entity.task import Task


# ------------------------------------------------------------------------------------------------ #
#                                        PIPELINE                                                  #
# ------------------------------------------------------------------------------------------------ #
class Pipeline(ABC):
    """
    Pipeline serves as an abstract base class for creating data processing pipelines. It manages the configuration,
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
            data = self._run_pipeline()
        elif self.endpoint_exists():
            data = self.read_endpoint()
        else:
            data = self._run_pipeline()

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
    def _run_pipeline(self) -> Any:
        """
        Abstract method for running the pipeline. Subclasses must implement this method to define the actual
        pipeline execution process.

        Returns:
        --------
        Any:
            The result of the pipeline execution, typically the final output after all tasks have been completed.
        """
        pass

    @task_announcer
    @profiler
    @cachenow
    def _run_task(self, data: Any, task: Task) -> Any:
        return task.run(data=data)


# ------------------------------------------------------------------------------------------------ #
#                                  PIPELINE BUILDER                                                #
# ------------------------------------------------------------------------------------------------ #
class PipelineBuilder(ABC):
    """
    Abstract base class for constructing pipelines. This class provides the structure for
    building a pipeline that orchestrates the execution of various tasks in a workflow.
    Subclasses must implement the `create_pipeline` method to define the specific pipeline
    to be created.

    Attributes:
    -----------
    _config : StageConfig
        Configuration object containing settings and environment-specific details necessary for building the pipeline.

    pipeline_cls : type[Pipeline]
        The class of the pipeline to be created. Defaults to the base `Pipeline` class but
        can be overridden to create a specific type of pipeline.

    logger : logging.Logger
        A logger instance used for logging information and debugging throughout the pipeline construction process.

    Methods:
    --------
    __init__(config: StageConfig, pipeline_cls: type[Pipeline] = Pipeline, **kwargs) -> None
        Initializes the `PipelineBuilder` with the provided configuration and optionally specifies
        a custom pipeline class.

    create_pipeline() -> Pipeline
        Abstract method that must be implemented by subclasses to create and return an instance of a pipeline.
        This pipeline will handle the execution of a series of tasks in the workflow.

    Parameters:
    -----------
    config : StageConfig
        The configuration object containing the necessary settings for the pipeline.

    pipeline_cls : type[Pipeline], optional
        The class of the pipeline to be created (defaults to `Pipeline`).

    **kwargs : dict
        Additional arguments that can be passed for further customization of the pipeline creation process.
    """

    def __init__(
        self,
        pipeline_cls: type[Pipeline],
        config: StageConfig,
        **kwargs,
    ) -> None:
        """
        Initializes the `PipelineBuilder` with the provided configuration and an optional custom pipeline class.

        Parameters:
        -----------
        config : StageConfig
            Configuration object containing necessary settings for the pipeline's execution.

        pipeline_cls : type[Pipeline], optional
            The class of the pipeline to be created (defaults to `Pipeline`).

        **kwargs : dict
            Additional keyword arguments for further customization during initialization.
        """
        self._config = config
        self._pipeline_cls = pipeline_cls
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def create_pipeline(self) -> Pipeline:
        """
        Creates and configures the ingestion pipeline with the necessary tasks.

        This method sets up the pipeline by adding tasks such as reading from the source,
        ingesting data, and writing to the target. It returns a fully constructed
        pipeline ready for execution.

        Returns:
        --------
        IngestPipeline:
            The fully configured data ingestion pipeline with all tasks.
        """
        # Instantiate pipeline
        pipe = self._pipeline_cls(config=self._config)
        # Extract the task and task configs, configure and add to pipeline.
        for task_config in self._config.task_configs:
            self.logger.debug(task_config)
            task = task_config.task(task_config)
            pipe.add_task(task)

        return pipe
