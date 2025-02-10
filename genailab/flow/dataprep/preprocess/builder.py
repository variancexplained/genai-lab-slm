#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab                                                                           #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/flow/dataprep/preprocess/builder.py                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday January 1st 2025 05:01:45 am                                              #
# Modified   : Saturday February 8th 2025 10:43:03 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""Acquire Stage Builder Module"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Union

from pyspark.sql import SparkSession

from genailab.asset.dataset.builder import DatasetBuilder
from genailab.asset.dataset.config import DatasetConfig
from genailab.core.dtypes import DFType
from genailab.core.flow import PhaseDef, StageDef
from genailab.flow.base.builder import StageBuilder
from genailab.flow.base.task import Task
from genailab.flow.dataprep.preprocess.stage import PreprocessStage


# ------------------------------------------------------------------------------------------------ #
class PreprocessStageBuilder(StageBuilder):
    """
    Builder class for constructing the preprocess stage of the data pipeline.

    This class defines methods for configuring and building tasks related to file preprocession,
    such as encoding, datatype casting, newline removal, and datetime conversion.
    It extends the abstract `StageBuilder` class and provides concrete implementations
    for the `phase`, `stage`, `reset`, and `build` methods.

    Attributes:
        _source_config (Union[DatasetConfig, None]): Configuration for the source dataset.
        _target_config (Union[DatasetConfig, None]): Configuration for the target dataset.
        _spark (Optional[SparkSession]): Spark session used for Spark-based datasets.
        _tasks (List[Task]): List of tasks to be executed in the stage.
        _task_configs (List[Dict[str, Any]]): Configurations for the tasks.
        _encoding (Optional[Dict[str, str]]): Configuration for encoding tasks.
        _datatypes (Optional[Dict[str, str]]): Configuration for datatype casting tasks.
        _newlines (Optional[Dict[str, str]]): Configuration for newline removal tasks.
        _datetime (Optional[Dict[str, str]]): Configuration for datetime conversion tasks.
        _logger (Logger): Logger instance for the class.
    """

    __PHASE = PhaseDef.DATAPREP
    __STAGE = StageDef.PREPROCESS
    __DFTYPE = DFType.PANDAS

    def __init__(self) -> None:
        """
        Initializes an instance of PreprocessStageBuilder and sets up initial configurations.
        """
        super().__init__()
        self._source_config: Union[DatasetConfig, None] = None
        self._target_config: Union[DatasetConfig, None] = None
        self._spark: Optional[SparkSession] = None
        self._tasks: List[Task] = []
        self._task_configs: List[Dict[str, Any]] = []

        self._encoding: Optional[Dict[str, str]] = None
        self._datatypes: Optional[Dict[str, str]] = None
        self._newlines: Optional[Dict[str, str]] = None
        self._datetime: Optional[Dict[str, str]] = None

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.reset()

    @property
    def phase(self) -> PhaseDef:
        """
        The phase of the pipeline associated with the preprocess stage.

        Returns:
            PhaseDef: The phase associated with the pipeline.
        """
        return self.__PHASE

    @property
    def stage(self) -> StageDef:
        """
        The stage of the pipeline associated with the preprocess stage.

        Returns:
            StageDef: The stage associated with the pipeline.
        """
        return self.__STAGE

    @property
    def dftype(self) -> DFType:
        """
        Defines the dataframe type of the pipeline.

        Returns:
            DFType: The dataframe type used in the pipeline.
        """
        return self.__DFTYPE

    def reset(self) -> None:
        """
        Resets the internal state of the builder by clearing configurations and tasks.

        Ensures a clean state for the next build process.
        """
        self._source_config = self._get_fileset_config(
            phase=self.phase, stage=self.stage, config="source_config"
        )
        self._target_config = self._get_dataset_config(
            phase=self.phase, stage=self.stage, config="target_config"
        )
        self._encoding = None
        self._datatypes = None
        self._newlines = None
        self._datetime = None

        self._task_configs = self._get_stage_config(
            phase=self.phase, stage=self.stage, config="tasks"
        )
        self._tasks = []

    def encoding(self) -> PreprocessStageBuilder:
        """
        Configures the encoding task for the preprocess stage.

        Returns:
            PreprocessStageBuilder: The builder instance for method chaining.
        """
        self._encoding = self._task_configs["encoding"]
        return self

    def datatypes(self) -> PreprocessStageBuilder:
        """
        Configures the datatype casting task for the preprocess stage.

        Returns:
            PreprocessStageBuilder: The builder instance for method chaining.
        """
        self._datatypes = self._task_configs["datatypes"]
        return self

    def newlines(self) -> PreprocessStageBuilder:
        """
        Configures the newline removal task for the preprocess stage.

        Returns:
            PreprocessStageBuilder: The builder instance for method chaining.
        """
        self._newlines = self._task_configs["newlines"]
        return self

    def datetime(self) -> PreprocessStageBuilder:
        """
        Configures the datetime conversion task for the preprocess stage.

        Returns:
            PreprocessStageBuilder: The builder instance for method chaining.
        """
        self._datetime = self._task_configs["datetime"]
        return self

    def build(
        self,
        source_config: Optional[DatasetConfig] = None,
        target_config: Optional[DatasetConfig] = None,
    ) -> PreprocessStageBuilder:
        """
        Builds the preprocess stage by validating configurations and assembling tasks.

        Args:
            source_config (Optional[DatasetConfig]): An optional configuration object for
                the source dataset. If not provided, the method falls back to the source
                configuration defined in the stage YAML config.
            target_config (Optional[DatasetConfig]): An optional configuration object for
                the target dataset. If not provided, the method falls back to the target
                configuration defined in the stage YAML config.

        Returns:
            PreprocessStageBuilder: The builder instance with the constructed stage.

        Raises:
            ValueError: If validation fails due to missing or invalid configurations.
        """
        self._validate()
        self._tasks = self._build_tasks()
        stage = PreprocessStage(
            source_config=source_config or self._source_config,
            target_config=target_config or self._target_config,
            tasks=self._tasks,
            repo=self._repo,
            dataset_builder=DatasetBuilder(),
            spark=self._spark,
        )
        self.reset()
        return stage

    def _build_tasks(self) -> List[Task]:
        """
        Constructs the list of tasks to be executed during the preprocess stage.

        Returns:
            List[Task]: The list of constructed tasks.
        """
        tasks = []
        tasks.append(self._task_builder.build(self._encoding))
        tasks.append(self._task_builder.build(self._datatypes))
        tasks.append(self._task_builder.build(self._newlines))
        tasks.append(self._task_builder.build(self._datetime))
        return tasks

    def _validate(self) -> None:
        """
        Validates the configurations and settings for the preprocess stage.

        Ensures that required fields such as encoding, datatypes, newline removal, and datetime tasks are defined.

        Raises:
            ValueError: If any required field is missing or invalid.
        """
        super()._validate()
        errors = []
        if not isinstance(self._source_config, DatasetConfig):
            errors.append("Source fileset config is required for the PreprocessStage.")
        if not isinstance(self._target_config, DatasetConfig):
            errors.append("Target dataset config is required for the PreprocessStage.")
        if self._encoding is None:
            errors.append("The encoding step is required for the PreprocessStage.")
        if self._datatypes is None:
            errors.append("The datatypes step is required for the PreprocessStage.")
        if self._newlines is None:
            errors.append(
                "The newlines removal step is required for the PreprocessStage."
            )
        if errors:
            msg = "\n".join(errors)
            self._logger.error(msg)
            raise ValueError(msg)

    def _get_fileset_config(
        self, phase: PhaseDef, stage: StageDef, config: str
    ) -> DatasetConfig:
        """
        Retrieves the fileset configuration for the preprocess stage.

        Args:
            phase (PhaseDef): The phase for which the configuration is being retrieved.
            stage (StageDef): The stage for which the configuration is being retrieved.
            config (str): The configuration key to retrieve.

        Returns:
            DatasetConfig: The retrieved fileset configuration.
        """
        fileset_config = self._get_stage_config(phase=phase, stage=stage, config=config)
        return DatasetConfig.from_dict(config=fileset_config)
