#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/domain/base/task.py                                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday September 10th 2024 04:49:44 pm                                             #
# Modified   : Saturday September 14th 2024 06:08:39 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Abstract Base Classes for the Task Subclasses w/in the Domain Services Layer"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any

from discover.domain.value_objects.config import ServiceConfig
from discover.domain.value_objects.context import Context


# ------------------------------------------------------------------------------------------------ #
#                                           TASK                                                   #
# ------------------------------------------------------------------------------------------------ #
class Task(ABC):
    """
    Abstract base class for tasks in a pipeline.

    This class defines the blueprint for tasks, requiring subclasses to implement
    the `run` method and specify the task's `stage`. Each task is initialized with
    a configuration and context, which are used to track and manage the task's execution.

    Attributes:
    -----------
    _config : ServiceConfig
        Configuration object that defines environment and task-specific settings.
    _context : Context
        Context object that holds metadata such as service type, service name, and stage
        during task execution.
    _logger : logging.Logger
        Logger instance for logging task-specific events and errors.
    """

    def __init__(
        self,
        *args,
        context: Context,
        config: ServiceConfig,
        **kwargs,
    ) -> None:
        """
        Initializes the Task with the given configuration and context.

        The configuration determines the environment and any other setup needed for the task,
        while the context tracks execution details such as service type and stage.

        Parameters:
        -----------
        *args :
            Positional arguments passed during initialization.
        context : Context
            The context object used to track task-specific metadata.
        config : ServiceConfig
            Configuration object that provides task-specific settings.
        **kwargs :
            Additional keyword arguments passed during initialization.
        """
        self._config = config
        self._context = context
        self._context.service_type = "Task"
        self._context.service_name = self.name
        self._context.stage = self.stage
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @property
    def logger(self) -> logging.Logger:
        """
        Provides read-only access to the logger instance for this task.

        This logger is used for logging events and errors during the task's execution.

        Returns:
        --------
        logging.Logger
            The logger instance associated with this task.
        """
        return self._logger

    @property
    def name(self) -> str:
        """
        Returns the name of the task, which is the class name of the task.

        This name is used in the context to track the task's execution.

        Returns:
        --------
        str:
            The name of the class implementing the task.
        """
        return self.__class__.__name__

    @property
    def context(self) -> Context:
        """
        Provides read-only access to the context object for this task.

        The context tracks the task's metadata, including its service type,
        name, and stage during execution.

        Returns:
        --------
        Context:
            The context object for this task.
        """
        return self._context

    @property
    @abstractmethod
    def stage(self) -> str:
        """
        Abstract property that must be implemented by subclasses to define the current stage of the task.

        The stage represents the lifecycle stage of the task, such as "INGEST", "TRANSFORM", or "LOAD".

        Returns:
        --------
        str:
            The current stage of the task.
        """
        pass

    @abstractmethod
    def run(self, *args: Any, **kwargs: Any) -> Any:
        """
        Abstract method that defines the behavior of the task when executed.

        This method must be implemented by any subclass to define the task's specific logic.
        It also updates the context with the task's service type, name, and stage,
        and generates a run ID for the task.

        Parameters:
        -----------
        *args :
            Positional arguments for the task's execution.
        **kwargs :
            Keyword arguments for the task's execution.

        Returns:
        --------
        Any:
            The result of the task's execution, depending on the implementation in the subclass.
        """
        self._context.service_type = "Task"
        self._context.service_name = self.name
        self._context.stage = self.stage
        self._context.create_run(owner=self)
