#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /discover/domain/service/base/pipeline.py                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday June 30th 2024 03:42:28 am                                                   #
# Modified   : Saturday September 14th 2024 03:43:52 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any

from discover.domain.service.base.task import Task
from discover.domain.value_objects.config import ServiceConfig
from discover.domain.value_objects.context import Context
from discover.domain.value_objects.lifecycle import Stage


# ------------------------------------------------------------------------------------------------ #
#                                        PIPELINE                                                  #
# ------------------------------------------------------------------------------------------------ #
class Pipeline(ABC):
    """Pipeline to manage and execute a sequence of tasks."""

    def __init__(
        self,
        config: ServiceConfig,
        context: Context,
    ):
        self._config = config
        self._context = context
        self._context.service_type = "Pipeline"
        self._context.service_name = self.name
        self._context.stage = self.stage
        self._tasks = []
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @property
    def logger(self) -> logging.Logger:
        return self._logger

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @property
    def context(self) -> Context:
        return self._context

    @property
    @abstractmethod
    def stage(self) -> Stage:
        """Returns the pipeline stage."""

    def add_task(self, task: Task):
        """Add a task to the pipeline.

        Args:
            task (Task): A task object to add to the pipeline.
        """
        self._tasks.append(task)

    def run(self) -> Any:
        """Execute the pipeline tasks in sequence."""
        # Adds a run_id to the pipeline context.
        self._context.create_run(owner=self)
        self._logger.debug(f"Checking if endpoint for {self.name} exists.")
        if self.endpoint_exists():
            self._logger.info(
                f"Endpoint {self.name} exists. Returning data from endpoint."
            )
            return self.read_endpoint()
        else:
            self._logger.debug(f"Endpoint does not exist. Running {self.name}.")
            data = None
            for task in self._tasks:
                data = task.run(data)
            return data

    def endpoint_exists(self) -> bool:
        """Returns True if the target already exists. False otherwise"""
        repo = self._config.target_data_config.repo

        return repo.exists(
            stage=self._config.target_data_config.stage,
            name=self._config.target_data_config.name,
        )

    def read_endpoint(self) -> Any:
        """Reads and returns the target data."""
        repo = self._config.target_data_config.repo
        return repo.get(
            stage=self._config.target_data_config.stage,
            name=self._config.target_data_config.name,
        )


# ------------------------------------------------------------------------------------------------ #
#                                  PIPELINE BUILDER                                                #
# ------------------------------------------------------------------------------------------------ #
class PipelineBuilder(ABC):
    """Abstract base class for constructing pipelines that execute preprocessing tasks.

    This class provides a blueprint for building a pipeline, which consists of a series
    of preprocessing tasks. Subclasses must implement the `create_pipeline` method to
    define the specific pipeline creation logic.

    Attributes:
        config (ServiceConfig): Configuration object that defines parameters for building the pipeline.
        pipeline_cls (type[Pipeline]): Class representing the pipeline to be constructed. Defaults to `Pipeline`.
        logger (logging.Logger): Logger instance for logging events related to the pipeline construction.
    """

    def __init__(
        self,
        config: ServiceConfig,
        context: Context,
        pipeline_cls: type[Pipeline] = Pipeline,
        **kwargs,
    ) -> None:
        """
        Initializes the PipelineBuilder with the given configuration and pipeline class.

        Args:
            config (ServiceConfig): Configuration object to be used for constructing the pipeline.
            pipeline_cls (type[Pipeline], optional): Pipeline class to use for creating the pipeline instance.
                                                     Defaults to `Pipeline`.
            **kwargs: Additional keyword arguments to be passed during the construction process.
        """
        self._config = config
        self._context = context

        self.pipeline_cls = pipeline_cls
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @abstractmethod
    def create_pipeline(self) -> Pipeline:
        """Constructs the pipeline that executes the preprocessing tasks.

        This method must be implemented by subclasses to define the logic for creating
        a pipeline. The pipeline is responsible for executing a series of tasks as part
        of the preprocessing stage.

        Returns:
            Pipeline: An instance of the pipeline to be executed.
        """
