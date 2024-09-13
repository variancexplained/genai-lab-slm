#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/domain/service/base/task.py                                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday September 10th 2024 04:49:44 pm                                             #
# Modified   : Friday September 13th 2024 02:33:44 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Abstract Base Classes for the Task Subclasses w/in the Domain Services Layer"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any, Optional

from discover.domain.value_objects.context import Context
from discover.domain.value_objects.lifecycle import Stage
from discover.infra.config.config import Config

# ------------------------------------------------------------------------------------------------ #
#                                           TASK                                                   #
# ------------------------------------------------------------------------------------------------ #


class Task(ABC):
    """
    Abstract base class for all tasks. This class provides a structure for
    defining tasks that have a configuration, environment, and stage, while
    enforcing the implementation of a `run` method in subclasses.

    Attributes:
    -----------
    _config : Config
        The configuration object used to set up the task's environment and stage.
    _env : str
        The environment in which the task operates, as determined by the configuration.
    _stage : str
        The stage of the task, if applicable.
    _logger : logging.Logger
        A private logger instance used for logging within the task. Subclasses can
        access this via a read-only property.

    Properties:
    -----------
    logger : logging.Logger (read-only)
        Provides read-only access to the logger instance for subclasses.

    Methods:
    --------
    name:
        Returns the name of the class implementing the task.

    stage:
        Returns the current stage of the task, if set.

    env:
        Returns the environment in which the task is executed.

    run:
        Abstract method that must be implemented by subclasses to define the
        specific behavior of the task.
    """

    def __init__(
        self,
        *args,
        config_cls: type[Config] = Config,
        stage: Optional[Stage] = None,
        **kwargs,
    ) -> None:
        """
        Initializes the Task with a configuration object. The configuration
        determines the environment and any other setup needed for the task.

        Parameters:
        -----------
        *args :
            Positional arguments passed during initialization.
        config_cls : type[Config], optional
            A class reference to the configuration class, which defaults to
            the `Config` class.
        **kwargs :
            Keyword arguments passed during initialization.
        """
        self._config = config_cls()
        self._stage = stage  # TODO: Figure out stage setting.
        self._context = Context(
            service_type="Task", service_name=self.name, stage=self.stage
        )
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @property
    def logger(self) -> logging.Logger:
        """
        Provides read-only access to the private logger instance. This logger
        is available for use in subclasses but cannot be modified directly.

        Returns:
        --------
        logging.Logger
            The logger instance associated with this task.
        """
        return self._logger

    @property
    def name(self) -> str:
        """
        Returns the name of the task, which is the class name of the task
        implementing this abstract class.

        Returns:
        --------
        str:
            The name of the class implementing the task.
        """
        return self.__class__.__name__

    @property
    def context(self) -> Context:
        return self._context

    @property
    @abstractmethod
    def stage(self) -> str:
        """
        Returns the current stage of the task, if any.

        Returns:
        --------
        str:
            The stage of the task, or None if no stage has been set.
        """

    @abstractmethod
    def run(self, *args: Any, **kwargs: Any) -> Any:
        """
        Abstract method that must be implemented by any subclass of Task.
        Defines the behavior of the task when it is executed.

        Parameters:
        -----------
        *args :
            Positional arguments for the task's execution.
        **kwargs :
            Keyword arguments for the task's execution.

        Returns:
        --------
        Any:
            The result of the task's execution, which depends on the implementation
            in the subclass.
        """
        pass
