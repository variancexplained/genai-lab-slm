#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/flow/base/builder.py                                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday January 1st 2025 05:02:14 am                                              #
# Modified   : Wednesday January 29th 2025 02:40:14 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Type, Union

from dependency_injector.wiring import Provide, inject
from genailab.asset.dataset.builder import DatasetBuilder
from genailab.asset.dataset.config import DatasetConfig
from genailab.container import GenAILabContainer
from genailab.core.dtypes import DFType
from genailab.core.flow import PhaseDef, StageDef
from genailab.flow.base.stage import Stage
from genailab.flow.base.task import Task, TaskBuilder
from genailab.infra.config.flow import FlowConfigReader
from genailab.infra.persist.repo.dataset import DatasetRepo
from genailab.infra.service.spark.pool import SparkSessionPool
from pyspark.sql import SparkSession


# ------------------------------------------------------------------------------------------------ #
class StageBuilder(ABC):
    """Abstract base class for building stages in a data pipeline.

    The `StageBuilder` class defines the structure and behavior for creating
    specific stages in a data pipeline. It manages configurations, datasets,
    Spark sessions, and tasks required to construct and validate stages.

    Args:
        repo (DatasetRepo): Repository for managing dataset operations.
            Default is injected from `GenAILabContainer.io.repo`.
        state (FlowState): State object for tracking the flow of the pipeline.
            Default is injected from `GenAILabContainer.io.flowstate`.
        spark_session_pool (SparkSessionPool): Pool for managing Spark sessions.
            Default is injected from `GenAILabContainer.spark.session_pool`.
        config_reader_cls (Type[FlowConfigReader]): Class used for reading
            pipeline configurations. Default is `FlowConfigReader`.
        dataset_builder_cls (Type[DatasetBuilder]): Class used for constructing datasets.
            Default is `DatasetBuilder`.
        task_builder_cls (Type[TaskBuilder]): Class used for constructing tasks.
            Default is `TaskBuilder`.

    Attributes:
        _repo (DatasetRepo): The dataset repository instance.
        _state (FlowState): The flow state object for managing pipeline state.
        _spark_session_pool (SparkSessionPool): Pool for Spark session management.
        _config_reader (FlowConfigReader): Reader for accessing pipeline configurations.
        _dataset_builder (DatasetBuilder): Builder for creating datasets.
        _task_builder (TaskBuilder): Builder for creating tasks.
        _source_config (DatasetConfig): Configuration for the source dataset.
        _target_config (DatasetConfig): Configuration for the target dataset.
        _spark (SparkSession): Spark session instance for distributed processing.
        _tasks (list): List of tasks required for the stage.
        _task_configs (list): List of task configurations.
        _logger (logging.Logger): Logger for capturing logs.
    """

    @inject
    def __init__(
        self,
        repo: DatasetRepo = Provide[GenAILabContainer.io.repo],
        spark_session_pool: SparkSessionPool = Provide[
            GenAILabContainer.spark.session_pool
        ],
        config_reader_cls: Type[FlowConfigReader] = FlowConfigReader,
        dataset_builder_cls: Type[DatasetBuilder] = DatasetBuilder,
        task_builder_cls: Type[TaskBuilder] = TaskBuilder,
    ) -> None:
        self._repo = repo
        self._spark_session_pool = spark_session_pool
        self._config_reader = config_reader_cls()
        self._dataset_builder = dataset_builder_cls()
        self._task_builder = task_builder_cls()

        self._source_config: Union[DatasetConfig, None] = None
        self._target_config: Union[DatasetConfig, None] = None
        self._spark: Optional[SparkSession] = None
        self._tasks: List[Task] = []
        self._task_configs: List[Dict[str, Any]] = []

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        self.reset()

    @property
    @abstractmethod
    def phase(self) -> PhaseDef:
        """
        Abstract property to define the phase of the pipeline.

        Returns:
            PhaseDef: The phase associated with the pipeline.
        """
        pass

    @property
    @abstractmethod
    def stage(self) -> StageDef:
        """
        Abstract property to define the stage of the pipeline.

        Returns:
            StageDef: The stage associated with the pipeline.
        """
        pass

    def reset(self) -> None:
        """
        Resets the internal state of the builder.

        This method initializes or resets configurations, Spark sessions,
        tasks, and task configurations to prepare the builder for constructing
        a stage.
        """
        self._source_config = self._get_dataset_config(
            phase=self.phase, stage=self.stage, config="source_config"
        )
        self._target_config = self._get_dataset_config(
            phase=self.phase, stage=self.stage, config="target_config"
        )

        self._task_configs = self._get_config(
            phase=self.phase, stage=self.stage, config="tasks"
        )
        self._tasks: List[Task] = []

    @abstractmethod
    def build(self, *args, **kwargs) -> Stage:
        """
        Abstract method to construct the stage.

        This method must be implemented by subclasses to define how
        the stage is built.

        Returns:
            Stage: The constructed stage object.
        """
        pass

    def _validate(self, *args, **kwargs) -> None:
        """
        Validates the source and target dataset passports.

        Ensures that both the source and target configurations contain valid
        `DatasetPassport` objects. Logs an error and raises a `ValueError`
        if any validation fails.

        Raises:
            ValueError: If either source or target passports are invalid.
        """
        pass

    def _get_config(
        self, phase: PhaseDef, stage: StageDef, config: str
    ) -> Dict[str, Any]:
        """
        Retrieves configuration details for a given phase and stage.

        Args:
            phase (PhaseDef): The phase of the pipeline.
            stage (StageDef): The stage of the pipeline.
            config (str): The specific configuration to retrieve
                (e.g., `source_config`, `target_config`, or `tasks`).

        Returns:
            Dict[str, Any]: The configuration details.

        Raises:
            KeyError: If the specified configuration is not found.
            RuntimeError: If an unrecognized error occurs during retrieval.
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

    def _get_dataset_config(
        self, phase: PhaseDef, stage: StageDef, config: str
    ) -> DatasetConfig:
        """
        Retrieves the dataset configuration for a given phase and stage.

        Args:
            phase (PhaseDef): The phase of the pipeline.
            stage (StageDef): The stage of the pipeline.
            config (str): The specific dataset configuration to retrieve.

        Returns:
            DatasetConfig: The dataset configuration object.
        """
        dataset_config = self._get_config(phase=phase, stage=stage, config=config)
        return DatasetConfig.from_dict(config=dataset_config)

    def _get_spark(self, dftype: DFType) -> SparkSession:
        """
        Retrieves a Spark session for the specified data frame type.

        Args:
            dftype (DFType): The type of data frame (e.g., PySpark, Pandas).

        Returns:
            SparkSession: A Spark session instance.
        """
        self._spark_session_pool.stop()
        return self._spark_session_pool.get_spark_session(dftype=dftype)
