#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/domain/service/base/service.py                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday September 14th 2024 03:24:28 am                                            #
# Modified   : Saturday September 14th 2024 03:29:13 am                                            #
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

    def __init__(self, config: ServiceConfig, context: Context) -> None:
        self._config = config
        self._context = context

    @abstractmethod
    def run(self) -> Any:
        """Abstract run method."""
