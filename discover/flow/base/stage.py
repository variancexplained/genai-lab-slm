#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/base/stage.py                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday September 20th 2024 08:14:05 pm                                              #
# Modified   : Saturday November 16th 2024 02:40:17 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Stage Base Class  Module"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List

from discover.core.flow import PhaseDef, StageDef
from discover.core.namespace import NestedNamespace
from discover.flow.base.task import Task, TaskBuilder


# ------------------------------------------------------------------------------------------------ #
#                                        STAGE                                                     #
# ------------------------------------------------------------------------------------------------ #
class Stage(ABC):
    """Abstract base class for pipeline stages.

    This class provides the foundation for creating and executing stages in a data pipeline.
    A stage typically involves loading source data, applying a series of tasks to the data,
    and saving the processed data to a destination. The stage is configurable with source
    and destination settings and a list of tasks to be executed.

    Attributes:
        _source_config (NestedNamespace): Configuration dictionary for the source dataset.
        _destination_config (NestedNamespace): Configuration dictionary for the destination dataset.
        _task_builder (Type[TaskBuilder]): The builder class responsible for constructing tasks.
        _tasks (List[Task]): A list of tasks to be executed in this stage.
        _force (bool): Whether to force execution even if the destination dataset exists.

    Methods:
        phase (PhaseDef): The phase of the pipeline.
        stage (StageDef): The specific stage of the pipeline.
        run() -> str: Executes the pipeline stage.
        build(stage_config: dict, force: bool = False) -> Stage: Creates and returns a new stage instance.
    """

    _task_builder = TaskBuilder

    def __init__(
        self,
        source_config: dict,
        destination_config: dict,
        tasks: List[Task],
        force: bool = False,
    ) -> None:
        """Initializes the Stage pipeline with the provided configuration and tasks.

        Args:
            source_config (dict): Configuration for the source dataset.
            destination_config (dict): Configuration for the destination dataset.
            tasks (List[Task]): List of tasks to be applied in the pipeline stage.
            task_builder (Type[TaskBuilder], optional): The task builder class to use for constructing tasks.
                Defaults to `TaskBuilder`.
            force (bool, optional): Whether to force execution even if the destination dataset exists.
                Defaults to `False`.

        Raises:
            ValueError: If source_config or destination_config are invalid or incomplete.
            RuntimeError: For other initialization errors.
        """

        self._source_config = NestedNamespace(source_config)
        self._destination_config = NestedNamespace(destination_config)
        self._tasks = tasks
        self._force = force

    @property
    @abstractmethod
    def phase(self) -> PhaseDef:
        """Returns the phase of the pipeline.

        This property is meant to categorize the stage within the larger pipeline process.
        """
        pass

    @property
    @abstractmethod
    def stage(self) -> StageDef:
        """Returns the specific stage within the pipeline.

        This property helps identify which stage of the pipeline is being executed.
        """
        pass

    @abstractmethod
    def run(self) -> str:
        """Executes the stage and returns the resulting asset ID.

        This method orchestrates the execution of the tasks defined in the stage,
        processes the source data, and saves the result to the destination.

        Returns:
            str: The asset ID of the generated dataset.

        Raises:
            RuntimeError: If an error occurs during the execution of the stage.
        """
        pass

    @classmethod
    def build(cls, stage_config: dict, force: bool = False) -> Stage:
        """Creates and returns a new stage instance from the provided configuration.

        Args:
            stage_config (dict): The configuration dictionary containing source, destination, and tasks details.
            force (bool, optional): Whether to force execution even if the destination dataset exists. Defaults to `False`.

        Returns:
            Stage: A new instance of the Stage class, configured with the provided settings.

        Raises:
            KeyError: If the required keys are missing from the stage_config.
            ValueError: If tasks cannot be built from the configuration.
            RuntimeError: If there is an error creating the stage.
        """
        try:
            tasks = [
                cls._task_builder.build(task_config)
                for task_config in stage_config["tasks"]
            ]
            return cls(
                source_config=stage_config["source_config"],
                destination_config=stage_config["destination_config"],
                tasks=tasks,
                force=force,
            )
        except KeyError as e:
            raise ValueError(
                f"Missing required configuration key in stage_config: {str(e)}"
            ) from e
        except Exception as e:
            raise RuntimeError(
                f"Failed to build stage from configuration: {str(e)}"
            ) from e
