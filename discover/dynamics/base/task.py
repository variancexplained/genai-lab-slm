#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/dynamics/base/task.py                                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday September 10th 2024 04:49:44 pm                                             #
# Modified   : Saturday September 21st 2024 11:45:21 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Abstract Base Classes for the Task Subclasses w/in the Domain Services Layer"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional

from discover.element.entity.config.service import StageConfig
from discover.element.entity.context.base import Context


# ------------------------------------------------------------------------------------------------ #
#                                   TASK CONFIG                                                    #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class TaskConfig(Config):
    """
    Configuration class for defining a task within the pipeline.

    This class holds the configuration for a task, ensuring that the task is a valid subclass of `Task`.
    It extends the base `Config` class, inheriting its validation logic while adding task-specific checks.

    Attributes:
    -----------
    task : Type[Task]
        A Task class type representing the task to be executed. The class must be a subclass of `Task`.

    force : bool:
        Whether to force execution of Task when cache exists.

    Methods:
    --------
    validate() -> None:
        Validates the task configuration by ensuring that `task` is a valid subclass of `Task`.
        It inherits the base validation from `Config` and raises an `InvalidConfigException`
        if the task type is invalid.
    """

    task: Type[Task] = Task
    force: bool = False

    def _validate(self) -> list:
        """
        Validates the TaskConfig.

        This method performs validation to ensure that `task` is a subclass of `Task`. If `task` is
        not a subclass of `Task`, an `InvalidConfigException` is raised with a descriptive error message.
        It also calls the `validate()` method of the base `Config` class to ensure that any other
        required configuration elements are valid.

        Raises:
        -------
        InvalidConfigException:
            If `task` is not a valid subclass of `Task`, or if the base configuration fails validation.
        """
        errors = super()._validate()

        if not isinstance(self.force, bool):
            errors.append(
                f"Invalid {self.__class__.__name__}. Expected a boolean type for force. Encountered {type(self.force).__name__}."
            )

        return errors


# ------------------------------------------------------------------------------------------------ #
#                                           TASK                                                   #
# ------------------------------------------------------------------------------------------------ #
class Task(ABC):
    """
    Abstract base class representing a generic task in a pipeline.

    This class serves as a template for defining tasks in a pipeline. It provides properties
    and methods to access the task's configuration, context, and logging utilities.
    Subclasses must implement the `run` method, which defines the task's execution logic.

    Attributes:
    -----------
    _config : Optional[StageConfig]
        Configuration object for the task, passed during initialization.
    _pipeline_context : Context
        The pipeline context, providing metadata about the pipeline's phase, stage, and task.
    _kwargs : dict
        Additional keyword arguments passed during initialization.
    _context : Context
        Task-specific context, derived from the pipeline context, which tracks the task's execution metadata.
    _logger : logging.Logger
        Logger instance used for logging events, errors, and other relevant information during task execution.

    Parameters:
    -----------
    pipeline_context : Context
        The context object representing the current pipeline's execution phase, stage, and task.
    config : Optional[StageConfig], optional
        Configuration object specific to the service or task, by default None.
    *args :
        Positional arguments passed to the task.
    **kwargs :
        Keyword arguments passed to the task.

    Methods:
    --------
    logger() -> logging.Logger:
        Provides access to the logger instance for this task.

    name() -> str:
        Returns the name of the task, typically the class name of the task.

    config() -> StageConfig:
        Returns the configuration object for the task.

    context() -> Context:
        Provides access to the task-specific context, which tracks the task's metadata.

    run(*args: Any, **kwargs: Any) -> Any:
        Abstract method that must be implemented by subclasses, defining the task's logic during execution.
    """

    def __init__(
        self,
        *args,
        config: Optional[StageConfig] = None,
        **kwargs,
    ) -> None:

        self._config = config
        self._kwargs = kwargs
        from discover.element.entity.context.task import TaskContext

        # Create task context from pipeline context stage
        self._context = TaskContext(
            ephase=config.service_context.ephase,
            estage=config.service_context.estage,
            task=type(self),
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
    def config(self) -> StageConfig:
        """
        Returns the configuration object for this task.

        If no configuration is provided during initialization, it returns None.

        Returns:
        --------
        StageConfig:
            The configuration object for this task.
        """
        return self._config

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
        pass
