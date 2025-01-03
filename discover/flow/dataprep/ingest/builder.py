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
# Modified   : Thursday January 2nd 2025 08:12:22 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""Acquire Stage Builder Module"""
from __future__ import annotations

from typing import Dict, List

from discover.asset.dataset.dataset import Dataset
from discover.flow.base.builder import StageBuilder
from discover.flow.base.stage import Stage
from discover.flow.base.task import Task


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

    def __init__(self) -> None:
        """
        Initializes the IngestStageBuilder with default settings and task configuration.

        Args:
            None
        """
        super().__init__()
        self._encoding = None
        self._datatypes = None
        self._newlines = None
        self._convert_datetime_utc = None

        self._config = self._get_task_config()

    def source_filepath(
        self, filepath: str = None, from_config: bool = True
    ) -> IngestStageBuilder:
        """
        Sets the source file path for the Ingest stage, either from a configuration or directly.

        This method configures the source file path for the data pipeline. If the `from_config` parameter
        is set to `True`, the file path is loaded from a predefined configuration based on the environment.
        If `from_config` is `False`, the file path is set directly using the provided `filepath` argument.

        Args:
            filepath (str, optional): The file path to the source data. This argument is used only if `from_config` is `False`.
            from_config (bool, optional): Determines whether to load the file path from the environment-specific configuration.
                If `True`, the file path is loaded from the configuration; otherwise, it is set directly using the `filepath` argument.

        Returns:
            IngestStageBuilder: The builder instance for method chaining, allowing further configurations or task additions.

        Raises:
            ValueError: If `from_config` is `False` and no `filepath` is provided.
        """

        if from_config:
            self._source_filepath = self._get_source_filepath()
        else:
            self._source_filepath = filepath
        return self

    def encoding(self) -> IngestStageBuilder:
        """
        Configures the encoding task for the Ingest stage.

        Returns:
            IngestStageBuilder: The builder instance for method chaining.
        """
        self._encoding = self._config["encoding"]
        return self

    def datatypes(self) -> IngestStageBuilder:
        """
        Configures the datatype casting task for the Ingest stage.

        Returns:
            IngestStageBuilder: The builder instance for method chaining.
        """
        self._datatypes = self._config["datatypes"]
        return self

    def newlines(self) -> IngestStageBuilder:
        """
        Configures the newline removal task for the Ingest stage.

        Returns:
            IngestStageBuilder: The builder instance for method chaining.
        """
        self._newlines = self._config["newlines"]
        return self

    def convert_datetime_utc(self) -> IngestStageBuilder:
        """
        Configures the datetime conversion task for the Ingest stage.

        Returns:
            IngestStageBuilder: The builder instance for method chaining.
        """
        self._convert_datetime_utc = self._config["convert_datetime_utc"]
        return self

    def build(self) -> IngestStageBuilder:
        """
        Builds the Ingest stage by validating configurations, constructing datasets,
        and assembling tasks.

        Returns:
            IngestStageBuilder: The builder instance with the constructed stage.
        """
        self._validate()
        self._source = self._build_source_dataset()
        self._target = self._build_target_dataset()
        self._tasks = self._build_tasks()
        self._stage = Stage(
            source=self._source,
            target=self._target,
            tasks=self._tasks,
            state=self._state,
            repo=self._repo,
        )
        return self

    def _build_source_dataset(self) -> Dataset:
        """
        Builds the source dataset from the provided passport and filepath.

        Returns:
            Dataset: The constructed source dataset.
        """
        dataset = (
            self._dataset_builder_from_file.passport(self._source_passport)
            .source_filepath(self._source_filepath)
            .build()
            .dataset
        )
        return dataset

    def _build_target_dataset(self) -> Dataset:
        """
        Builds the target dataset from the provided passport.

        Returns:
            Dataset: The constructed target dataset.
        """
        dataset = self._dataset_builder.passport(self._target_passport).build().dataset
        return dataset

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
        tasks.append(self._task_builder.build(self._convert_datetime_utc))
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
        if not isinstance(self._source_filepath, str):
            errors.append("Source filepath is required for the IngestStage.")
        if self._encoding is None:
            errors.append("The encoding step is required for the IngestStage.")
        if self._datatypes is None:
            errors.append("The datatypes step is required for the IngestStage.")
        if self._newlines is None:
            errors.append("The newlines removal step is required for the IngestStage.")
        if self._convert_datetime_utc is None:
            errors.append(
                "The convert_datetime_utc step is required for the IngestStage."
            )

        if errors:
            msg = "\n".join(errors)
            self._logger.error(msg)
            raise ValueError(msg)

    def _get_source_filepath(self) -> str:
        try:
            return self._config_reader.get_config(section="phases", namespace=False)[
                "dataprep"
            ]["stages"]["ingest"]["source_config"]["filepath"]
        except KeyError as e:
            msg = f"Configuration Error. Unable to obtain source filepath from config. Check your config.yaml file.\n{e}"
            self._logger.error(msg)
            raise RuntimeError(msg)
        except Exception as e:
            msg = f"Unrecognized error occurred while accessing the source file configuration.\n{e}"
            self._logger.error(msg)
            raise RuntimeError(msg)

    def _get_task_config(self) -> Dict[str, Dict[str, str]]:
        """
        Retrieves the configuration for tasks in the Ingest stage from the config reader.

        Returns:
            Dict[str, Dict[str, str]]: The task configuration dictionary.
        """
        try:
            return self._config_reader.get_config(section="phases", namespace=False)[
                "dataprep"
            ]["stages"]["ingest"]["tasks"]
        except KeyError as e:
            msg = f"Configuration Error. Unable to obtain task configurations from config. Check your config.yaml file.\n{e}"
            self._logger.error(msg)
            raise RuntimeError(msg)
        except Exception as e:
            msg = f"Unrecognized error occurred while accessing the task configuration.\n{e}"
            self._logger.error(msg)
            raise RuntimeError(msg)
