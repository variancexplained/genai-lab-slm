#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/stage/base.py                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday September 20th 2024 08:14:05 pm                                              #
# Modified   : Tuesday December 24th 2024 09:42:37 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Stage Base Class  Module"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod

from dependency_injector.wiring import Provide, inject

from discover.asset.dataset import Dataset
from discover.container import DiscoverContainer
from discover.core.flow import PhaseDef, StageDef
from discover.flow.task.base import Task, TaskBuilder
from discover.infra.workspace.service import WorkspaceService


# ------------------------------------------------------------------------------------------------ #
#                                        STAGE                                                     #
# ------------------------------------------------------------------------------------------------ #
class Stage(ABC):
    """Abstract base class representing a stage in a pipeline.

    A stage encapsulates a set of tasks and provides methods to manage tasks and execute the stage.
    It must be extended by concrete implementations that define the specific behavior of the stage.

    Args:
        phase (PhaseDef): The phase of the pipeline to which this stage belongs.
        stage (StageDef): The specific stage within the pipeline.
        workspace_service (WorkspaceService, optional): The service managing workspace operations.
            Defaults to the injected workspace service.
        force (bool, optional): Whether to force execution, overriding default conditions. Defaults to False.
        **kwargs: Additional keyword arguments for extended functionality.

    Attributes:
        _phase (PhaseDef): The phase of the pipeline.
        _stage (StageDef): The specific stage within the pipeline.
        _workspace_service (WorkspaceService): The workspace service for operations.
        _force (bool): Indicates whether forced execution is enabled.
        _tasks (list): List of tasks added to this stage.
        _logger (logging.Logger): Logger for the stage.
    """

    _task_builder = TaskBuilder

    @inject
    def __init__(
        self,
        phase: PhaseDef,
        stage: StageDef,
        workspace_service: WorkspaceService = Provide[
            DiscoverContainer.workspace.service
        ],
        force: bool = False,
        **kwargs,
    ) -> None:
        self._phase = phase
        self._stage = stage
        self._workspace_service = workspace_service
        self._force = force
        self._tasks = []
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @property
    def phase(self) -> PhaseDef:
        """Returns the phase of the pipeline.

        Returns:
            PhaseDef: The phase associated with this stage.
        """
        return self._phase

    @property
    def stage(self) -> StageDef:
        """Returns the specific stage within the pipeline.

        Returns:
            StageDef: The stage associated with this instance.
        """
        return self._stage

    def add_task(self, task: Task) -> None:
        """Adds a task to the stage and assigns the stage identifier to the task.

        Args:
            task (Task): The task to be added to the stage.

        Raises:
            TypeError: If the task is not an instance of the expected Task class.
        """
        if not isinstance(task, Task):
            raise TypeError(
                f"Expected an instance or subclass of Task. Received type {type(task)}"
            )
        self._tasks.append(task)

    @abstractmethod
    def run(self) -> Dataset:
        """Executes the stage and returns the resulting dataset.

        This method must be implemented by subclasses.

        Returns:
            Dataset: The output dataset after executing the stage.
        """
        pass

    @classmethod
    @abstractmethod
    def build(
        cls,
        stage_config: dict,
        force: bool = False,
        **kwargs,
    ) -> Stage:
        """Builds a stage instance based on the provided configuration.

        This method must be implemented by subclasses.

        Args:
            stage_config (dict): Configuration for the stage.
            force (bool, optional): Whether to force execution, overriding default conditions. Defaults to False.
            **kwargs: Additional keyword arguments for extended functionality.

        Returns:
            Stage: An instance of the concrete stage.
        """
        pass
