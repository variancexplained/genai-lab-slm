#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/substance/entity/config/task.py                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday September 19th 2024 09:04:41 pm                                            #
# Modified   : Friday September 20th 2024 05:22:57 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Abstract Base Classes for the Task Subclasses w/in the Domain Services Layer"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Type

from discover.substance.entity.config.base import Config
from discover.substance.entity.task import Task


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
