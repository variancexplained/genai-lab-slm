#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/base/builder.py                                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday January 1st 2025 05:02:14 am                                              #
# Modified   : Friday January 3rd 2025 07:02:06 am                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Type

from dependency_injector.wiring import Provide, inject

from discover.asset.dataset.builder import DatasetBuilder
from discover.container import DiscoverContainer
from discover.flow.base.stage import Stage
from discover.flow.base.task import TaskBuilder
from discover.infra.config.flow import FlowConfigReader
from discover.infra.persist.object.flowstate import FlowState
from discover.infra.persist.repo.dataset import DatasetRepo
from discover.infra.service.spark.pool import SparkSessionPool


# ------------------------------------------------------------------------------------------------ #
class StageBuilder(ABC):
    """
    Abstract base class for constructing a data processing pipeline stage.

    The `StageBuilder` class facilitates the configuration and construction of a `Stage` object.
    It manages dependencies for dataset creation, task building, and metadata handling, ensuring
    all required components are in place before building the stage.

    Args:
        config_reader_cls (Type[FlowConfigReader], optional): Class responsible for reading flow configurations.
            Defaults to `FlowConfigReader`.
        repo (DatasetRepo, optional): Repository used to manage datasets.
            Defaults to the provided `DiscoverContainer.io.dataset_repo`.
        state (FlowState, optional): The state managing metadata and flow details.
            Defaults to the provided `DiscoverContainer.io.flowstate`.
        dataset_builder_cls (Type[DatasetBuilder], optional): Class used for building datasets.
            Defaults to `DatasetBuilder`.
        dataset_builder_from_file_cls (Type[DatasetBuilder], optional): Class used for building datasets from files.
            Defaults to `DatasetBuilder`.
        task_builder_cls (Type[TaskBuilder], optional): Class used for building tasks in the pipeline.
            Defaults to `TaskBuilder`.

    Attributes:
        _config_reader (FlowConfigReader): Instance responsible for reading flow configurations.
        _repo (DatasetRepo): Repository used to manage datasets.
        _state (FlowState): Instance responsible for managing metadata and flow states.
        _dataset_builder (DatasetBuilder): Instance used for constructing datasets.
        _dataset_builder_from_file (DatasetBuilder): Instance used for building datasets from files.
        _task_builder (TaskBuilder): Instance used for constructing tasks.
        _source (Optional[Dataset]): The source dataset for the stage (initially `None`).
        _source_passport (Optional[DatasetPassport]): The passport for the source dataset (initially `None`).
        _tasks (List[Task]): A list of tasks for the pipeline (initially empty).
        _task_configs (List[Any]): Configuration details for tasks (initially empty).
        _target (Optional[Dataset]): The target dataset for the stage (initially `None`).
        _target_passport (Optional[DatasetPassport]): The passport for the target dataset (initially `None`).
        _logger (Logger): Logger instance for tracking stage construction and errors.

    Methods:
        stage() -> Stage:
            Abstract property that, when implemented, returns the constructed `Stage`.

        reset() -> None:
            Resets the internal state of the builder, clearing all datasets, passports, tasks, and configurations.

        source_passport(passport: DatasetPassport) -> StageBuilder:
            Sets the source passport for the stage and returns the builder instance for chaining.

        target_passport(passport: DatasetPassport) -> StageBuilder:
            Sets the target passport for the stage and returns the builder instance for chaining.

        build() -> StageBuilder:
            Abstract method that must be implemented in subclasses to build the `Stage`.

        _validate() -> None:
            Validates the passports for the source and target datasets. If they are invalid, raises a `ValueError`.
    """

    @inject
    def __init__(
        self,
        config_reader_cls: Type[FlowConfigReader] = FlowConfigReader,
        repo: DatasetRepo = Provide[DiscoverContainer.io.dataset_repo],
        state: FlowState = Provide[DiscoverContainer.io.flowstate],
        dataset_builder_cls: Type[DatasetBuilder] = DatasetBuilder,
        dataset_builder_from_file_cls: Type[DatasetBuilder] = DatasetBuilder,
        spark_session_pool: SparkSessionPool = Provide[
            DiscoverContainer.spark.session_pool
        ],
        task_builder_cls: Type[TaskBuilder] = TaskBuilder,
    ) -> None:
        self._config_reader = config_reader_cls()
        self._repo = repo
        self._state = state
        self._dataset_builder = dataset_builder_cls()
        self._dataset_builder_from_file = dataset_builder_from_file_cls()
        self._spark_session_pool = spark_session_pool
        self._task_builder = task_builder_cls()

        self._source = None
        self._tasks = []
        self._task_configs = []
        self._target = None
        self._target_passport = None

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @property
    def stage(self) -> Stage:
        """
        Abstract property to be implemented in subclasses to return the constructed stage.

        Returns:
            Stage: The built stage object.
        """
        stage = self._stage
        self.reset()
        return stage

    def reset(self) -> None:
        """
        Resets the internal state of the builder by clearing source, target, passports, and tasks.

        This ensures a clean state for the next stage build process.
        """
        self._source = None
        self._tasks = []
        self._task_configs = []
        self._target = None
        self._target_passport = None

    @abstractmethod
    def build(self) -> StageBuilder:
        """
        Abstract method to be implemented in subclasses for building the stage.

        Returns:
            StageBuilder: The current instance of the builder for method chaining.
        """
        pass

    def _validate(self) -> None:
        """
        Validates the source and target passports.

        Ensures that both passports are valid instances of `DatasetPassport`.
        If any passport is invalid, raises a `ValueError` and logs the error.

        Raises:
            ValueError: If either the source or target passport is not a valid `DatasetPassport`.
        """
        pass
