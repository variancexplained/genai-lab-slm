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
# Modified   : Friday January 17th 2025 09:53:27 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""Acquire Stage Builder Module"""
from __future__ import annotations

from typing import List

from discover.asset.dataset.builder import DatasetBuilder
from discover.asset.dataset.identity import DatasetConfig
from discover.core.flow import PhaseDef, StageDef
from discover.flow.base.builder import StageBuilder
from discover.flow.base.task import Task
from discover.flow.dataprep.ingest.stage import IngestStage


# ------------------------------------------------------------------------------------------------ #
class IngestStageBuilder(StageBuilder):
    """
    A builder class for constructing the Ingest stage of a data pipeline.

    This class facilitates the construction of an Ingest stage, which processes
    datasets by applying various transformation tasks like encoding, datatype casting,
    newline removal, and converting datetime to UTC. The builder validates configuration
    settings, constructs source and target datasets, and assembles the list of tasks to
    execute during the stage.

    Attributes:
        _encoding (dict): Configuration for the encoding task.
        _datatypes (dict): Configuration for the datatypes task.
        _newlines (dict): Configuration for the newlines task.
        _convert_datetime_utc (dict): Configuration for the datetime conversion task.
        _config (dict): Configuration read from the config reader.
    """

    __PHASE = PhaseDef.DATAPREP
    __STAGE = StageDef.INGEST

    def __init__(self) -> None:
        """
        Initializes the IngestStageBuilder with default settings and task configuration.

        Args:
            None
        """
        super().__init__()
        self._source_config = None
        self._target_config = None
        self._encoding = None
        self._datatypes = None
        self._newlines = None
        self._convert_datetime_utc = None

        self._task_config = self._get_config(
            phase=self.__PHASE, stage=self.__STAGE, config="tasks"
        )

    def reset(self) -> None:
        super().reset()
        self._source_config = None
        self._target_config = None
        self._encoding = None
        self._datatypes = None
        self._newlines = None
        self._convert_datetime_utc = None

    def source(self, source_config: DatasetConfig) -> IngestStageBuilder:
        """
        Sets the source config, including the file path for the Ingest stage.

        Args:
            source_config (DatasetConfig): Dataset config including the source filepath

        Returns:
            IngestStageBuilder: The builder instance for method chaining, allowing further configurations or task additions.

        """
        self._source_config = source_config
        return self

    def target(self, target_config: DatasetConfig) -> IngestStageBuilder:
        """
        Sets the target dataset config.

        Args:
            target_config (DatasetConfig): Target dataset config.

        Returns:
            IngestStageBuilder: The builder instance for method chaining, allowing further configurations or task additions.

        """
        self._target_config = target_config
        return self

    def encoding(self) -> IngestStageBuilder:
        """
        Configures the encoding task for the Ingest stage.

        Returns:
            IngestStageBuilder: The builder instance for method chaining.
        """
        self._encoding = self._task_config["encoding"]
        return self

    def datatypes(self) -> IngestStageBuilder:
        """
        Configures the datatype casting task for the Ingest stage.

        Returns:
            IngestStageBuilder: The builder instance for method chaining.
        """
        self._datatypes = self._task_config["datatypes"]
        return self

    def newlines(self) -> IngestStageBuilder:
        """
        Configures the newline removal task for the Ingest stage.

        Returns:
            IngestStageBuilder: The builder instance for method chaining.
        """
        self._newlines = self._task_config["newlines"]
        return self

    def convert_datetime_utc(self) -> IngestStageBuilder:
        """
        Configures the datetime conversion task for the Ingest stage.

        Returns:
            IngestStageBuilder: The builder instance for method chaining.
        """
        self._convert_datetime_utc = self._task_config["convert_datetime_utc"]
        return self

    def build(self) -> IngestStageBuilder:
        """
        Builds the Ingest stage by validating configurations, constructing datasets,
        and assembling tasks.

        Returns:
            IngestStageBuilder: The builder instance with the constructed stage.
        """
        self._validate()
        self._tasks = self._build_tasks()
        self._stage = IngestStage(
            source_config=self._source_config,
            target_config=self._target_config,
            tasks=self._tasks,
            state=self._state,
            repo=self._repo,
            dataset_builder=DatasetBuilder(),
        )
        return self

    def _build_tasks(self) -> List[Task]:
        """
        Constructs a list of tasks to be executed during the Ingest stage.

        Returns:
            List[Task]: The list of constructed tasks.
        """
        tasks = []
        tasks.append(self._task_builder.build(self._encoding))
        tasks.append(self._task_builder.build(self._datatypes))
        tasks.append(self._task_builder.build(self._newlines))
        return tasks

    def _validate(self) -> None:
        """
        Validates the configurations and settings for the Ingest stage.

        Ensures that required fields such as the source filepath, encoding, datatypes,
        and datetime conversion tasks are defined.

        Raises:
            ValueError: If any required field is missing or invalid.
        """
        super()._validate()
        errors = []
        if not isinstance(self._source_config, DatasetConfig):
            errors.append("Source dataset config is required for the IngestStage.")
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

    def _get_source_config(self) -> str:
        return self._get_config(
            phase=self.__PHASE, stage=self.__STAGE, config="source_config"
        )
