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
# Modified   : Saturday September 14th 2024 05:05:50 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from abc import ABC, abstractmethod
from typing import Any

from discover.domain.value_objects.config import ServiceConfig
from discover.domain.value_objects.context import Context


# ------------------------------------------------------------------------------------------------ #
class DomainService(ABC):
    """
    Abstract base class for domain services.

    This class provides a blueprint for domain-level services, which are responsible
    for executing business logic in the domain layer. Subclasses of `DomainService`
    must implement the `run` method to define the specific logic for the service.

    Attributes:
    -----------
    _config : ServiceConfig
        Configuration object that provides service-specific settings and environment information.
    _context : Context
        Context object that holds metadata such as service type, name, and stage,
        and tracks the execution of the domain service.
    """

    def __init__(self, config: ServiceConfig, context: Context) -> None:
        """
        Initializes the domain service with the provided configuration and context.

        Parameters:
        -----------
        config : ServiceConfig
            Configuration object containing settings and environment information for the service.
        context : Context
            Context object that tracks the service's execution details, such as service type and stage.
        """
        self._config = config
        self._context = context

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
        pass
