#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/domain/entity/config/base.py                                              #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday September 10th 2024 04:49:55 pm                                             #
# Modified   : Thursday September 19th 2024 09:10:40 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Abstract Base Class for Data Processing Stage Configurations"""
from __future__ import annotations

import logging
from abc import abstractmethod
from dataclasses import dataclass

from discover.core.data import DataClass
from discover.domain.entity.context.service import ServiceContext
from discover.domain.exception.config import InvalidConfigException


# ------------------------------------------------------------------------------------------------ #
#                                    CONFIG                                                        #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class Config(DataClass):
    """
    Base configuration class for services within the pipeline.

    This class acts as a foundation for more specific configuration classes and ensures that
    the essential `service_context` is valid and properly configured.

    Attributes:
    -----------
    service_context : ServiceContext
        The execution context for the service, containing metadata about the current phase and stage.

    Methods:
    --------
    validate() -> None:
        Validates the configuration by ensuring that `service_context` is an instance of `ServiceContext`.
        Calls the `validate()` method of the `service_context` to ensure that its internal data is also valid.
        Raises an `InvalidConfigException` if any validation checks fail.
    """

    service_context: ServiceContext

    @abstractmethod
    def validate(self) -> None:
        """
        Validates the base configuration.

        Ensures that `service_context` is an instance of `ServiceContext` and calls its `validate()` method
        to perform deeper validation. If `service_context` is invalid or misconfigured, an `InvalidConfigException`
        is raised with an appropriate error message.

        Raises:
        -------
        InvalidConfigException:
            If `service_context` is not a valid instance of `ServiceContext`, or if the validation of the
            `service_context` fails.
        """
        errors = []
        if not isinstance(self.service_context, ServiceContext):
            errors.append(
                f"Invalid {self.__class__.__name__}. Expected a ServiceContext instance. Encountered {type(self.service_context).__name__}."
            )

        if errors:
            error_msg = "\n".join(errors)
            logging.error(error_msg)
            raise InvalidConfigException(error_msg)

        # Validate the service context itself
        self.service_context.validate()
