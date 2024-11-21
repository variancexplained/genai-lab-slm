#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/task/clean/text/factory.py                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 21st 2024 12:35:09 am                                             #
# Modified   : Thursday November 21st 2024 03:20:34 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Strategy Factory Module"""
from abc import ABC, abstractmethod
from typing import Type

from discover.flow.task.clean.base.strategy import DetectStrategy, RepairStrategy

# ------------------------------------------------------------------------------------------------ #


class TextStrategyFactory(ABC):
    """Factory to retrieve strategies for anomaly detection and repair."""

    @property
    @abstractmethod
    def detect_strategies(self) -> dict[str, Type[DetectStrategy]]:
        """Returns a dictionary of detect strategies"""

    @property
    @abstractmethod
    def repair_strategies(self) -> dict[str, Type[RepairStrategy]]:
        """Returns a dictionary of detect strategies"""
