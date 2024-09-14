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
# Modified   : Saturday September 14th 2024 03:49:58 am                                            #
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
    """"""

    def __init__(
        self,
        *args,
        context: Context,
        config: ServiceConfig,
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
        self._config = config
        self._context = context
        self._context.service_type = "Task"
        self._context.service_name = self.name
        self._context.stage = self.stage
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
        self._context.service_type + "Task"
        self._context.service_name = self.name
        self._context.stage = self.stage
        self._context.create_run(owner=self)
