#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/application/service/base/service.py                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday September 13th 2024 05:41:52 pm                                              #
# Modified   : Thursday September 19th 2024 01:11:54 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from abc import ABC, abstractmethod
from typing import Any, Optional

from discover.domain.entity.config import ServiceConfig


# ------------------------------------------------------------------------------------------------ #
class ApplicationService(ABC):
    """Abstract base class for application layer services.

    This class defines the interface for services in the application layer,
    ensuring that any subclass must implement the `run` method. The `run` method
    is intended to encapsulate the execution logic of a service.
    """

    def __init__(
        self,
        config: ServiceConfig,
    ) -> None:
        self._config = config

    @abstractmethod
    def run(self) -> Optional[Any]:
        """Runs an application layer service.

        This method must be implemented by any subclass to define the specific
        behavior of the service. The implementation of this method typically
        involves executing the core functionality of the service.
        """
