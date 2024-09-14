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
# Modified   : Saturday September 14th 2024 05:35:28 pm                                            #
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
    Abstract base class for tasks within a pipeline. The `Task` class provides common functionality
    such as logging and context management, and requires subclasses to implement the `run` method
    which defines the task-specific behavior.

    Attributes:
    -----------
    _config : ServiceConfig
        The configuration object containing settings and environment information for the task.

    _pipeline_context : Context
        The pipeline context that tracks execution metadata like stage and service type.

    _context : Context
        A task-specific context derived from the pipeline context, tracking task execution details
        such as service type, service name, and stage.

    _logger : logging.Logger
        Logger instance for logging task-specific events and errors.

    Methods:
    --------
    __init__(config: ServiceConfig, pipeline_context: Context, *args, **kwargs) -> None
        Initializes the task with the provided configuration and pipeline context.

    logger() -> logging.Logger
        Property that provides read-only access to the logger instance for the task.

    name() -> str
        Property that returns the class name of the task, used as the task's name.

    context() -> Context
        Property that provides read-only access to the task-specific context.

    run(*args: Any, **kwargs: Any) -> Any
        Abstract method that must be implemented by subclasses to define the task's behavior
        when executed. This method updates the context and executes the task logic.

    Parameters:
    -----------
    *args :
        Positional arguments passed during task initialization or execution.
    config : ServiceConfig
        The configuration object containing task-specific settings.
    pipeline_context : Context
        The context object used to track the pipeline's execution metadata, such as stage and service type.
    **kwargs :
        Additional keyword arguments passed during task initialization or execution.
    """

    def __init__(
        self,
        *args,
        config: ServiceConfig,
        pipeline_context: Context,
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
        self._pipeline_context = pipeline_context
        self._kwargs = kwargs

        # Create task context from pipeline context stage
        self._context = Context(
            process_type="Task",
            process_name=self.name,
            stage=self._pipeline_context.stage,
        )

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

    @abstractmethod
    def run(self, *args: Any, **kwargs: Any) -> Any:
        """
        Abstract method that defines the behavior of the task when executed.

        This method must be implemented by any subclass to define the task's specific logic.

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
