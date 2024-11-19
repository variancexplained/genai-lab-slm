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
# Modified   : Tuesday November 19th 2024 12:26:34 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Stage Base Class  Module"""
from __future__ import annotations

# ------------------------------------------------------------------------------------------------ #
#                                        STAGE                                                     #
# ------------------------------------------------------------------------------------------------ #
from abc import ABC, abstractmethod

from discover.core.flow import PhaseDef, StageDef
from discover.core.namespace import NestedNamespace
from discover.flow.base.task import Task, TaskBuilder


class Stage(ABC):
    """
    Abstract base class representing a stage in a data processing pipeline.

    This class defines the core properties and methods that must be implemented
    by all specific stage types. It manages tasks associated with the stage and
    provides functionality to execute those tasks and produce an asset.

    Args:
        phase (PhaseDef): The phase of the pipeline.
        stage (StageDef): The specific stage within the pipeline.
        source_config (dict): Configuration for the data source.
        destination_config (dict): Configuration for the data destination.
        force (bool): Whether to force execution, even if the output already exists.
    """

    _task_builder = TaskBuilder

    def __init__(
        self,
        phase: PhaseDef,
        stage: StageDef,
        source_config: dict,
        destination_config: dict,
        force: bool = False,
        **kwargs,
    ) -> None:
        self._phase = phase
        self._stage = stage
        self._source_config = NestedNamespace(source_config)
        self._destination_config = NestedNamespace(destination_config)
        self._force = force
        self._tasks = []

    @property
    def phase(self) -> PhaseDef:
        """Returns the phase of the pipeline."""
        return self._phase

    @property
    def stage(self) -> StageDef:
        """Returns the specific stage within the pipeline."""
        return self._stage

    def add_task(self, task: Task) -> None:
        """Adds a task to the stage and assigns the stage identifier to the task.

        Args:
            task (Task): The task to be added to the stage.

        Raises:
            TypeError: If the task is not an instance of the expected Task class.
        """
        if not isinstance(task, Task):
            raise TypeError("Expected an instance of Task.")
        task.stage = self.stage
        self._tasks.append(task)

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
    def build(cls, stage_config: dict, force: bool = False, **kwargs) -> Stage:
        """Creates and returns a new stage instance from the provided configuration.

        Args:
            stage_config (dict): The configuration dictionary containing source, destination, and tasks details.
            force (bool, optional): Whether to force execution even if the destination dataset exists. Defaults to False.

        Returns:
            Stage: A new instance of the Stage class, configured with the provided settings.

        Raises:
            KeyError: If the required keys are missing from the stage_config.
            ValueError: If tasks cannot be built from the configuration.
            RuntimeError: If there is an error creating the stage.
        """
        try:
            # Instantiate the Stage object
            stage = cls(
                phase=PhaseDef.from_value(stage_config["phase"]),
                stage=StageDef.from_value(stage_config["stage"]),
                source_config=stage_config["source_config"],
                destination_config=stage_config["destination_config"],
                force=force,
                **kwargs,
            )

            # Construct the Stage's Task objects
            tasks = [
                cls._task_builder.build(task_config=task_config)
                for task_config in stage_config["tasks"]
            ]

            # Add Tasks to the Stage object
            for task in tasks:
                stage.add_task(task)

            return stage

        except KeyError as e:
            raise ValueError(
                f"Missing required configuration key in stage_config: {str(e)}"
            ) from e
        except Exception as e:
            raise RuntimeError(
                f"Failed to build stage from configuration: {str(e)}"
            ) from e

    def _get_asset_id(self, config: NestedNamespace) -> str:
        """Returns the asset id given an asset configuration.

        Args:
            config (NestedNamespace): An asset configuration.

        Returns:
            str: The asset_id for the asset.
        """
        return self._asset_idgen.get_asset_id(
            asset_type=config.asset_type,
            phase=PhaseDef.from_value(config.phase),
            stage=StageDef.from_value(config.stage),
            name=config.name,
        )
