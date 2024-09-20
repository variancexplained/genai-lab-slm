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
# Modified   : Thursday September 19th 2024 09:01:09 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import logging
from dataclasses import dataclass

from discover.core.data import DataClass
from discover.domain.exception.context import InvalidContextException
from discover.domain.value_objects.lifecycle import Phase, Stage


# ------------------------------------------------------------------------------------------------ #
@dataclass
class Context(DataClass):
    """
    Represents a generic context that contains metadata for the current phase and stage of execution.

    Attributes:
    -----------
    phase : Phase
        The phase of the process (e.g., warmup, cook, etc.).

    stage : Stage
        The stage of execution within the phase (e.g., initialization, execution).

    Methods:
    --------
    validate() -> None:
        Validates the context by ensuring that both `phase` and `stage` are instances
        of their respective classes. Raises an `InvalidContextException` with detailed
        error messages if validation fails.
    """

    phase: Phase
    stage: Stage

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
        errors = []
        if not isinstance(self.phase, Phase):
            errors.append(
                f"Invalid {self.__class__.__name__}. Expected a Phase instance. Encountered {type(self.phase).__name__}."
            )
        if not isinstance(self.stage, Stage):
            errors.append(
                f"Invalid {self.__class__.__name__}. Expected a Stage instance. Encountered {type(self.stage).__name__}."
            )

        if errors:
            error_msg = "\n".join(errors)
            logging.error(error_msg)
            raise InvalidContextException(error_msg)
