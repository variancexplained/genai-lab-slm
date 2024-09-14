#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /discover/domain/base/pipeline.py                                                   #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday June 30th 2024 03:42:28 am                                                   #
# Modified   : Saturday September 14th 2024 06:17:08 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any

from discover.domain.base.task import Task
from discover.domain.value_objects.config import ServiceConfig
from discover.domain.value_objects.context import Context
from discover.domain.value_objects.lifecycle import Stage
from discover.infra.monitor.profiler import profiler


# ------------------------------------------------------------------------------------------------ #
#                                        PIPELINE                                                  #
# ------------------------------------------------------------------------------------------------ #
class Pipeline(ABC):
    """
    Abstract base class for managing and executing a sequence of tasks.

    This class represents a pipeline that organizes a series of tasks,
    executes them in sequence, and manages the context and configuration
    throughout the execution process. Subclasses must define the `stage` property
    to specify the pipeline's stage (e.g., INGEST, TRANSFORM).

    Attributes:
    -----------
    _config : ServiceConfig
        Configuration object for the pipeline, which provides settings and
        environment-specific details, including source and target repositories.
    _context : Context
        Context object used to track metadata related to the pipeline execution,
        such as service type, name, stage, and run ID.
    _tasks : List[Task]
        A list of tasks that the pipeline will execute in sequence.
    _logger : logging.Logger
        Logger instance used for logging pipeline-specific events and messages.
    """

    def __init__(
        self,
        config: ServiceConfig,
        context: Context,
    ):
        """
        Initializes the pipeline with the provided configuration and context.

        The pipeline tracks the context and configuration to manage the execution
        of tasks, and it automatically sets the service type, name, and stage.

        Parameters:
        -----------
        config : ServiceConfig
            Configuration object containing environment details, including the source and target repositories.
        context : Context
            Context object used to track execution metadata, including service type, name, stage, and run ID.
        """
        self._config = config
        self._context = context
        self._context.service_type = "Pipeline"
        self._context.service_name = self.name
        self._context.stage = self.stage
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

    @property
    def name(self) -> str:
        """
        Returns the name of the pipeline, which is the class name of the pipeline.

        Returns:
        --------
        str:
            The name of the class implementing the pipeline.
        """
        return self.__class__.__name__

    @property
    def context(self) -> Context:
        """
        Provides read-only access to the context object for this pipeline.

        The context tracks metadata such as service type, name, stage, and run ID.

        Returns:
        --------
        Context:
            The context object for this pipeline.
        """
        return self._context

    @property
    @abstractmethod
    def stage(self) -> Stage:
        """
        Abstract property that must be implemented by subclasses to define the pipeline's current stage.

        The stage represents the lifecycle stage of the pipeline, such as INGEST, TRANSFORM, or LOAD.

        Returns:
        --------
        Stage:
            The current stage of the pipeline.
        """
        pass

    def add_task(self, task: Task):
        """
        Adds a task to the pipeline's sequence of tasks.

        Parameters:
        -----------
        task : Task
            A task object to add to the pipeline for execution.
        """
        self._tasks.append(task)

    @profiler
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
        # Adds a run_id to the pipeline context.
        self._context.create_run(owner=self)

        if self._config.force:
            return self._run_pipeline()
        elif self.endpoint_exists():
            return self.read_endpoint()
        else:
            return self._run_pipeline()

    def endpoint_exists(self) -> bool:
        """
        Checks if the target endpoint for the pipeline already exists.

        Returns:
        --------
        bool:
            True if the target data already exists, False otherwise.
        """
        self._logger.debug(f"Checking if endpoint for {self.name} exists.")
        repo = self._config.target_data_config.repo

        return repo.exists(
            stage=self._config.target_data_config.stage,
            name=self._config.target_data_config.name,
        )

    def read_endpoint(self) -> Any:
        """
        Reads and returns the target data from the repository.

        If the target data already exists, it retrieves and returns it.

        Returns:
        --------
        Any:
            The target data from the repository.
        """
        self._logger.info(f"Endpoint {self.name} exists. Returning data from endpoint.")
        repo = self._config.target_data_config.repo
        return repo.get(
            stage=self._config.target_data_config.stage,
            name=self._config.target_data_config.name,
        )

    def _run_pipeline(self) -> Any:
        """
        Runs the pipeline by executing all the tasks in sequence.

        Each task is run in order, passing the result of one task to the next.

        Returns:
        --------
        Any:
            The result of the final task in the pipeline.
        """
        data = None
        for task in self._tasks:
            data = task.run(data)
        return data


# ------------------------------------------------------------------------------------------------ #
#                                  PIPELINE BUILDER                                                #
# ------------------------------------------------------------------------------------------------ #
class PipelineBuilder(ABC):
    """
    Abstract base class for constructing pipelines that execute a sequence of tasks.

    This class provides the foundation for building pipelines, each of which consists
    of a series of tasks. Subclasses must implement the `create_pipeline` method to
    define the specific logic for assembling and creating a pipeline. The pipeline
    is responsible for executing tasks related to data processing, transformations,
    and other stages of the pipeline lifecycle.

    Attributes:
    -----------
    _config : ServiceConfig
        Configuration object that contains parameters for building the pipeline,
        including task-specific settings and environment details.
    _context : Context
        Context object that holds metadata such as service type, name, and stage,
        and tracks the execution of the pipeline.
    pipeline_cls : type[Pipeline]
        The class representing the pipeline to be constructed. Defaults to the `Pipeline` base class.
    logger : logging.Logger
        Logger instance for logging events related to pipeline construction and task management.
    """

    def __init__(
        self,
        config: ServiceConfig,
        context: Context,
        pipeline_cls: type[Pipeline] = Pipeline,
        **kwargs,
    ) -> None:
        """
        Initializes the PipelineBuilder with the provided configuration, context, and pipeline class.

        Parameters:
        -----------
        config : ServiceConfig
            Configuration object containing settings and parameters for constructing the pipeline.
        context : Context
            Context object for tracking metadata related to the pipeline's execution.
        pipeline_cls : type[Pipeline], optional
            The class representing the pipeline to be constructed. Defaults to `Pipeline`.
        **kwargs :
            Additional keyword arguments that may be passed to customize the pipeline construction process.
        """
        self._config = config
        self._context = context
        self.pipeline_cls = pipeline_cls
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @abstractmethod
    def create_pipeline(self) -> Pipeline:
        """
        Constructs the pipeline that executes a series of tasks.

        Subclasses must implement this method to define the logic for creating a pipeline.
        The pipeline is responsible for executing a series of tasks, each of which contributes
        to processing, transformation, or other steps in the pipeline's workflow.

        Returns:
        --------
        Pipeline:
            An instance of the pipeline to be executed.
        """
        pass
