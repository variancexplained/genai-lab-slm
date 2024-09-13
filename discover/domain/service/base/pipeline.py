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
# Modified   : Friday September 13th 2024 02:31:29 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any

from discover.domain.service.base.config import StageConfig
from discover.domain.service.base.repo import Repo
from discover.domain.service.base.task import Task
from discover.domain.value_objects.context import Context
from discover.domain.value_objects.lifecycle import Stage


# ------------------------------------------------------------------------------------------------ #
#                                        PIPELINE                                                  #
# ------------------------------------------------------------------------------------------------ #
class Pipeline(ABC):
    """Pipeline to manage and execute a sequence of tasks."""

    def __init__(
        self,
        config: StageConfig,
        source_repo_cls: type[Repo],
        target_repo_cls: type[Repo],
    ):
        self._source_repo = source_repo_cls()
        self._target_repo = target_repo_cls()
        self._config = config
        self._tasks = []
        self._context = Context(
            service_type="Pipeline", service_name=self.name, stage=self.stage
        )
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
        self._logger.debug(f"Checking if endpoint for {self._name} exists.")
        if self.endpoint_exists():
            self._logger.info(
                f"Endpoint {self._name} exists. Returning data from endpoint."
            )
            return self.read_endpoint()
        else:
            self._logger.debug(f"Endpoint does not exist. Running {self._name}.")
            data = None
            for task in self._tasks:
                data = task.run(data)
            return data

    def endpoint_exists(self) -> bool:
        """Returns True if the target already exists. False otherwise"""

        return self._target_repo.exists(
            stage=self._config.target_stage,
            name=self._config.target_name,
        )

    def read_endpoint(self) -> Any:
        """Reads and returns the target data."""
        return self._target_repo.get(
            stage=self._config.target_stage, name=self.config.target_name
        )


# ------------------------------------------------------------------------------------------------ #
#                                  PIPELINE BUILDER                                                #
# ------------------------------------------------------------------------------------------------ #
class PipelineBuilder(ABC):
    """Abstract base class for data preprocessor classes

    Args:
        config (StageConfig): Configuration for the subclass stage.
        pipeline_cls type[Pipeline]: Pipeline class to instantiate
        review_repo_cls (type[ReviewRepo]): Manages dataset IO
        source_reader_cls (type[Reader]): Class for reading the source data.
        target_writer_cls (type[Writer]): Class for writing the target data
        target_reader_cls (type[Reader]): Class for reading the target data.
    """

    def __init__(
        self,
        config: StageConfig,
        source_repo_cls: type[Repo],
        target_repo_cls: type[Repo],
        pipeline_cls: type[Pipeline] = Pipeline,
        **kwargs,
    ) -> None:
        super().__init__()
        self.config = config
        self._source_repo_cls = source_repo_cls
        self._target_repo_cls = target_repo_cls
        self.pipeline_cls = pipeline_cls
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @abstractmethod
    def create_pipeline(self) -> Pipeline:
        """Constructs the pipeline that executes the preprocessing tasks."""
