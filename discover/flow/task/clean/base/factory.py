#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/task/clean/base/factory.py                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 21st 2024 12:35:09 am                                             #
# Modified   : Thursday November 21st 2024 01:11:45 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Strategy Factory Module"""
from abc import ABC, abstractmethod
from typing import Type

from discover.flow.task.clean.base.strategy import DetectStrategy, RepairStrategy

# ------------------------------------------------------------------------------------------------ #


class StrategyFactory(ABC):
    """Factory to retrieve strategies for anomaly detection and repair."""

    @property
    @abstractmethod
    def detect_strategies(self) -> dict[str, Type[DetectStrategy]]:
        """Returns a dictionary of detect strategies"""

    @property
    @abstractmethod
    def repair_strategies(self) -> dict[str, Type[RepairStrategy]]:
        """Returns a dictionary of detect strategies"""

    def get_detect_strategy(self, strategy_type) -> Type[DetectStrategy]:
        """
        Returns a detect strategy based on the type.

        Args:
            strategy_type (str): The type of detection strategy.
            **kwargs: Additional arguments required to initialize the strategy.

        Returns:
            DetectStrategy: An instance of the selected detect strategy.

        Raises:
            ValueError: If the strategy type is not supported.
        """
        try:
            return self.detect_strategies[strategy_type]
        except KeyError:
            raise ValueError(f"Unknown detect strategy type: {strategy_type}")

    def get_repair_strategy(self, strategy_type) -> Type[RepairStrategy]:
        """
        Returns a repair strategy based on the type.

        Args:
            strategy_type (str): The type of repair strategy (e.g., "normalize", "replace", "remove").
            **kwargs: Additional arguments required to initialize the strategy.

        Returns:
            RepairStrategy: An instance of the selected repair strategy.

        Raises:
            ValueError: If the strategy type is not supported.
        """
        try:
            return self.repair_strategies[strategy_type]
        except KeyError:
            raise ValueError(f"Unknown repair strategy type: {strategy_type}")
