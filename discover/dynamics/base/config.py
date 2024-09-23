#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/dynamics/base/config.py                                                   #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday September 10th 2024 04:49:55 pm                                             #
# Modified   : Sunday September 22nd 2024 08:18:42 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Abstract Base Class for Data Processing Stage Configurations"""
from __future__ import annotations

import logging
from abc import abstractmethod

# ------------------------------------------------------------------------------------------------ #
#                                    CONFIG                                                        #
# ------------------------------------------------------------------------------------------------ #
from dataclasses import dataclass
from typing import List

from discover.core.data_class import DataClass
from discover.element.entity.context.service import ServiceContext
from discover.element.exception.config import InvalidConfigException


@dataclass
class Config(DataClass):
    """
    Base configuration class for services within the pipeline.

    This class provides a standardized validation process for all subclasses. It ensures that subclasses
    implement a `validate()` method that returns a list of errors, which will be automatically logged and raised
    if validation fails. It also handles setting up the logger for each subclass.

    Attributes:
    -----------
    service_context : ServiceContext
        The execution context for the service, containing metadata about the current phase and stage.


    Methods:
    --------
    __post_init__() -> None:
        Initializes the logger, runs validation, and handles error logging and raising if needed.

    validate() -> list:
        Abstract method that must be implemented by subclasses to return a list of validation errors.
        Each subclass should define its own specific validation logic.

    log_and_raise(errors: List[str]) -> None:
        Logs any validation errors and raises an `InvalidConfigException` if errors are found.
    """

    service_context: ServiceContext

    def __post_init__(self) -> None:
        """
        Initializes the logger and runs validation. If any validation errors are found, they are logged
        and an exception is raised.
        """
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.validate()

    def validate(self) -> None:
        """Invokes the validation subclass specific validation logic."""
        errors = self._validate()
        self._log_and_raise(errors=errors)

    @abstractmethod
    def _validate(self) -> list:
        """
        Abstract method for subclasses to implement their specific validation logic.
        This method should return a list of validation errors, if any are found.
        """
        errors = []

        if not isinstance(self.service_context, ServiceContext):
            errors.append(
                f"Invalid {self.__class__.__name__}. Expected a ServiceContext instance. Encountered {type(self.service_context).__name__}."
            )
        return errors

    def _log_and_raise(self, errors: List[str]) -> None:
        """
        Logs the provided list of errors and raises an `InvalidConfigException` if errors are present.

        Parameters:
        -----------
        errors : List[str]
            A list of validation error messages.
        """
        if errors:
            error_msg = "\n".join(errors)
            self._logger.error(error_msg)
            raise InvalidConfigException(error_msg)
