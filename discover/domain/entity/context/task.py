#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/domain/entity/context/task.py                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday September 19th 2024 09:01:19 pm                                            #
# Modified   : Thursday September 19th 2024 09:03:25 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Task Context Module"""
import logging
from dataclasses import dataclass

from discover.domain.entity.context.base import Context
from discover.domain.entity.task import Task
from discover.domain.exception.context import InvalidContextException


# ------------------------------------------------------------------------------------------------ #
@dataclass
class TaskContext(Context):
    """
    Represents a context for task-specific operations, inheriting from `Context`.

    Attributes:
        task (type[Task]): The type of task being executed (e.g., ExtractTask, TransformTask, LoadTask).
    """

    task: type[Task]

    def validate(self) -> None:
        """
        Validates the context object.

        Ensures that `phase` is an instance of `Phase` and `stage` is an instance of `Stage`.
        If any of these attributes are invalid, an `InvalidContextException` is raised with
        a detailed error message.

        Raises:
        -------
        InvalidContextException: If `phase` or `stage` are not valid instances.
        """
        super().validate()
        errors = []
        if not issubclass(self.task, Task):
            errors.append(
                f"Invalid {self.__class__.__name__}. Expected a subclass of Task. Encountered {self.task.__name__}."
            )

        if errors:
            error_msg = "\n".join(errors)
            logging.error(error_msg)
            raise InvalidContextException(error_msg)
