#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/domain/base/service.py                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday September 14th 2024 03:24:28 am                                            #
# Modified   : Saturday September 14th 2024 05:19:07 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from abc import ABC, abstractmethod
from typing import Any

from discover.domain.value_objects.config import ServiceConfig


# ------------------------------------------------------------------------------------------------ #
class DomainService(ABC):
    """
    Abstract base class for a domain service that encapsulates the core business logic
    for a specific domain. This class serves as a blueprint for services, requiring subclasses
    to implement the `run` method, which defines the service's behavior.

    Attributes:
    -----------
    _config : ServiceConfig
        The configuration object containing settings and environment-specific details
        needed to run the service.

    Methods:
    --------
    __init__(config: ServiceConfig) -> None
        Initializes the domain service with the provided configuration.

    run() -> Any
        Abstract method that must be implemented by subclasses to define the service's behavior.
        This method contains the core business logic of the domain service and should be
        overridden in each subclass.

    Parameters:
    -----------
    config : ServiceConfig
        The configuration object that includes the settings and environment information for the service.
    """

    def __init__(self, config: ServiceConfig) -> None:
        """
        Initializes the domain service with the provided configuration.

        Parameters:
        -----------
        config : ServiceConfig
            Configuration object containing settings and environment information for the service.
        """
        self._config = config

    @abstractmethod
    def run(self) -> Any:
        """
        Abstract method that must be implemented by subclasses to define the service's behavior.

        The `run` method encapsulates the core business logic for the domain service.

        Returns:
        --------
        Any:
            The result of the domain service's execution, as defined by the subclass implementation.
        """
