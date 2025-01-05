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
# Modified   : Saturday January 4th 2025 09:16:30 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Type

from dependency_injector.wiring import Provide, inject
from pyspark.sql import SparkSession

from discover.asset.dataset.builder import DatasetBuilder
from discover.container import DiscoverContainer
from discover.core.dtypes import DFType
from discover.core.flow import PhaseDef, StageDef
from discover.flow.base.stage import Stage
from discover.flow.base.task import TaskBuilder
from discover.infra.config.flow import FlowConfigReader
from discover.infra.persist.object.flowstate import FlowState
from discover.infra.persist.repo.dataset import DatasetRepo


# ------------------------------------------------------------------------------------------------ #
class StageBuilder(ABC):
    """Abstract base class for building stages in a data processing pipeline.

    This class serves as a blueprint for constructing stages, which are integral
    components of a data processing workflow. It manages configuration reading,
    dataset handling, flow state management, task building, and Spark session pooling.

    Attributes:
        _config_reader (FlowConfigReader): Instance of the FlowConfigReader class for reading configuration.
        _repo (DatasetRepo): Repository instance for handling dataset operations.
        _state (FlowState): The current state of the flow, provided by the container.
        _dataset_builder (DatasetBuilder): Instance of the DatasetBuilder class for constructing datasets.
        _task_builder (TaskBuilder): Instance of the TaskBuilder class for creating tasks.
        _source_config (Optional[dict]): Configuration for the data source, initially set to None.
        _tasks (List[Any]): List of tasks built by this stage, initially empty.
        _task_configs (List[dict]): List of configurations for tasks, initially empty.
        _logger (logging.Logger): Logger for the StageBuilder class, initialized with the class name.

    Args:
        config_reader_cls (Type[FlowConfigReader]): Class responsible for reading the flow configuration.
        repo (DatasetRepo): Dataset repository provided by the dependency injection container.
        state (FlowState): Current state of the flow, injected by the container.
        dataset_builder_cls (Type[DatasetBuilder]): Class responsible for building datasets.
        task_builder_cls (Type[TaskBuilder]): Class responsible for building tasks.
    """

    @inject
    def __init__(
        self,
        config_reader_cls: Type[FlowConfigReader] = FlowConfigReader,
        repo: DatasetRepo = Provide[DiscoverContainer.io.dataset_repo],
        state: FlowState = Provide[DiscoverContainer.io.flowstate],
        dataset_builder_cls: Type[DatasetBuilder] = DatasetBuilder,
        task_builder_cls: Type[TaskBuilder] = TaskBuilder,
    ) -> None:
        self._config_reader = config_reader_cls()
        self._repo = repo
        self._state = state
        self._dataset_builder = dataset_builder_cls()
        self._task_builder = task_builder_cls()

        self._source_config = None
        self._spark = None
        self._tasks = []
        self._task_configs = []

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
        self._source_config = None
        self._tasks = []
        self._task_configs = []
        self._spark = None

    def spark(self, spark: SparkSession) -> StageBuilder:
        """
        Sets the spark session for the buildr

        Args:
            spark (SparkSession): Spark session.

        Returns:
            StageBuilder: The current instance of the builder for method chaining.
        """
        self._spark = spark
        return self

    # -------------------------------------------------------------------------------------------- #
    #                                 DATAFRAME TYPE                                               #
    # -------------------------------------------------------------------------------------------- #
    def as_pandas(self) -> StageBuilder:
        """
        Sets the dataframe type to pandas

        Returns:
            DatasetBuilder: The current builder instance for chaining.
        """
        self._dftype = DFType.PANDAS
        return self

    # -------------------------------------------------------------------------------------------- #
    def as_spark(self) -> StageBuilder:
        """
        Sets the dataframe type to spark

        Returns:
            DatasetBuilder: The current builder instance for chaining.
        """
        self._dftype = DFType.SPARK
        return self

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

    def _get_config(
        self, phase: PhaseDef, stage: StageDef, config: str
    ) -> Dict[str, Any]:
        """
        Retrieves the configuration for the stage source

        Args:
            phase (PhaseDef): The phase for which the source config is being retrieved
            stage (StageDef): The stage for which the source config is being retrieved
            config (str): Either `source_config`, `target_config`, or `tasks`.

        Returns:
            Dict[str,Dict[str,str]]: Dictionary containing the source configuration

        Raises:
            KeyError: if the configuration isn't found.
            Exception: if an unrecognized exception occurred.

        """
        try:
            return self._config_reader.get_config(section="phases", namespace=False)[
                phase.value
            ]["stages"][stage.value][config]
        except KeyError as e:
            msg = f"Configuration Error. Unable to obtain the {config} configuration from phase {phase.value} and stage {stage.value}. Check your config.yaml file.\n{e}"
            self._logger.error(msg)
            raise RuntimeError(msg)
        except Exception as e:
            msg = f"Unrecognized error occurred while accessing the {config} configuration from phase {phase.value} and stage {stage.value}.\n{e}"
            self._logger.error(msg)
            raise RuntimeError(msg)
