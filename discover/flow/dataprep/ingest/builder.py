#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/dataprep/ingest/builder.py                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday January 1st 2025 05:01:45 am                                              #
# Modified   : Thursday January 23rd 2025 06:39:07 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""Acquire Stage Builder Module"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Union

from pyspark.sql import SparkSession

from discover.asset.dataset.builder import DatasetBuilder
from discover.asset.dataset.config import DatasetConfig, FilesetConfig
from discover.core.dtypes import DFType
from discover.core.flow import PhaseDef, StageDef
from discover.flow.base.builder import StageBuilder
from discover.flow.base.task import Task
from discover.flow.dataprep.ingest.stage import IngestStage


# ------------------------------------------------------------------------------------------------ #
class IngestStageBuilder(StageBuilder):
    """
    Builder class for constructing the ingest stage of the data pipeline.

    This class defines methods for configuring and building tasks related to file ingestion,
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
    __STAGE = StageDef.INGEST

    def __init__(self) -> None:
        """
        Initializes an instance of IngestStageBuilder and sets up initial configurations.
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
        The phase of the pipeline associated with the ingest stage.

        Returns:
            PhaseDef: The phase associated with the pipeline.
        """
        return self.__PHASE

    @property
    def stage(self) -> StageDef:
        """
        The stage of the pipeline associated with the ingest stage.

        Returns:
            StageDef: The stage associated with the pipeline.
        """
        return self.__STAGE

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
        self._spark = (
            None
            if self._source_config.dftype == DFType.PANDAS
            else self._get_spark(dftype=self._source_config.dftype)
        )
        self._encoding = None
        self._datatypes = None
        self._newlines = None
        self._datetime = None

        self._task_configs = self._get_config(
            phase=self.phase, stage=self.stage, config="tasks"
        )
        self._tasks = []

    def encoding(self) -> IngestStageBuilder:
        """
        Configures the encoding task for the ingest stage.

        Returns:
            IngestStageBuilder: The builder instance for method chaining.
        """
        self._encoding = self._task_configs["encoding"]
        return self

    def datatypes(self) -> IngestStageBuilder:
        """
        Configures the datatype casting task for the ingest stage.

        Returns:
            IngestStageBuilder: The builder instance for method chaining.
        """
        self._datatypes = self._task_configs["datatypes"]
        return self

    def newlines(self) -> IngestStageBuilder:
        """
        Configures the newline removal task for the ingest stage.

        Returns:
            IngestStageBuilder: The builder instance for method chaining.
        """
        self._newlines = self._task_configs["newlines"]
        return self

    def datetime(self) -> IngestStageBuilder:
        """
        Configures the datetime conversion task for the ingest stage.

        Returns:
            IngestStageBuilder: The builder instance for method chaining.
        """
        self._datetime = self._task_configs["datetime"]
        return self

    def build(self) -> IngestStageBuilder:
        """
        Builds the ingest stage by validating configurations and assembling tasks.

        Returns:
            IngestStageBuilder: The builder instance with the constructed stage.

        Raises:
            ValueError: If validation fails due to missing or invalid configurations.
        """
        self._validate()
        self._tasks = self._build_tasks()
        stage = IngestStage(
            source_config=self._source_config,
            target_config=self._target_config,
            tasks=self._tasks,
            repo=self._repo,
            fao=self._fao,
            dataset_builder=DatasetBuilder(),
            spark=self._spark,
        )
        self.reset()
        return stage

    def _build_tasks(self) -> List[Task]:
        """
        Constructs the list of tasks to be executed during the ingest stage.

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
        Validates the configurations and settings for the ingest stage.

        Ensures that required fields such as encoding, datatypes, newline removal, and datetime tasks are defined.

        Raises:
            ValueError: If any required field is missing or invalid.
        """
        super()._validate()
        errors = []
        if not isinstance(self._source_config, FilesetConfig):
            errors.append("Source fileset config is required for the IngestStage.")
        if not isinstance(self._target_config, DatasetConfig):
            errors.append("Target dataset config is required for the IngestStage.")
        if self._encoding is None:
            errors.append("The encoding step is required for the IngestStage.")
        if self._datatypes is None:
            errors.append("The datatypes step is required for the IngestStage.")
        if self._newlines is None:
            errors.append("The newlines removal step is required for the IngestStage.")
        if errors:
            msg = "\n".join(errors)
            self._logger.error(msg)
            raise ValueError(msg)

    def _get_fileset_config(
        self, phase: PhaseDef, stage: StageDef, config: str
    ) -> FilesetConfig:
        """
        Retrieves the fileset configuration for the ingest stage.

        Args:
            phase (PhaseDef): The phase for which the configuration is being retrieved.
            stage (StageDef): The stage for which the configuration is being retrieved.
            config (str): The configuration key to retrieve.

        Returns:
            FilesetConfig: The retrieved fileset configuration.
        """
        fileset_config = self._get_config(phase=phase, stage=stage, config=config)
        return FilesetConfig.from_dict(config=fileset_config)
