#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/domain/entity/context/base.py                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday September 19th 2024 08:59:49 pm                                            #
# Modified   : Friday September 20th 2024 12:57:45 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import logging
from abc import abstractmethod

# ------------------------------------------------------------------------------------------------ #
from dataclasses import dataclass
from typing import List

from discover.core.data import DataClass
from discover.domain.exception.context import InvalidContextException
from discover.domain.value_objects.lifecycle import Phase, Stage


@dataclass
class Context(DataClass):
    """
    Base context class for handling phase and stage information within the pipeline.

    This class validates that the `phase` and `stage` attributes are instances of their respective
    types and provides standardized error logging and exception handling if validation fails.

    Attributes:
    -----------
    phase : Phase
        The current phase of the pipeline (must be an instance of `Phase`).
    stage : Stage
        The current stage of the pipeline (must be an instance of `Stage`).

    Methods:
    --------
    __post_init__() -> None:
        Initializes the logger, runs validation on `phase` and `stage`, and logs errors or raises
        an exception if validation fails.

    validate() -> list:
        Abstract method to be implemented by subclasses, returning a list of validation errors
        if `phase` or `stage` are of incorrect types.

    log_and_raise(errors: List[str]) -> None:
        Logs the provided list of errors and raises an `InvalidContextException` if errors are present.
    """

    phase: Phase
    stage: Stage

    def __post_init__(self) -> None:
        """
        Initializes the logger and performs validation. If validation errors are found, they are
        logged and an exception is raised.
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
        Performs validation to ensure that `phase` and `stage` are of the correct types.

        Returns:
        --------
        list:
            A list of validation errors, if any are found. If no errors are present, returns an empty list.
        """
        errors = []
        if not isinstance(self.phase, Phase):
            errors.append(
                f"Invalid {self.__class__.__name__}. Expected a Phase instance. Encountered {type(self.phase).__name__}."
            )
        if not isinstance(self.stage, Stage):
            errors.append(
                f"Invalid {self.__class__.__name__}. Expected a Stage instance. Encountered {type(self.stage).__name__}."
            )

        return errors

    def _log_and_raise(self, errors: List[str]) -> None:
        """
        Logs the provided list of errors and raises an `InvalidContextException` if errors are present.

        Parameters:
        -----------
        errors : List[str]
            A list of validation error messages.
        """
        if errors:
            error_msg = "\n".join(errors)
            self._logger.error(error_msg)
            raise InvalidContextException(error_msg)
