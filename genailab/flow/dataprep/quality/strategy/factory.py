#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/flow/dataprep/quality/strategy/factory.py                                 #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 21st 2024 12:35:09 am                                             #
# Modified   : Sunday January 26th 2025 10:41:24 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Strategy Factory Module"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, Type

# ------------------------------------------------------------------------------------------------ #


class StrategyFactory(ABC):
    """Factory to retrieve strategies for anomaly detection and repair."""

    @property
    @abstractmethod
    def detect_strategies(self) -> Dict[str, Type[DetectStrategy]]:
        """Returns a dictionary of detect strategies"""

    @property
    @abstractmethod
    def repair_strategies(self) -> Dict[str, Type[RepairStrategy]]:
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


# ------------------------------------------------------------------------------------------------ #


class DetectStrategy(ABC):
    """
    Abstract base class for detection strategies.

    Detection strategies are used to identify anomalies in data based on
    predefined or dynamic rules.
    """

    @abstractmethod
    def detect(self, data):
        """
        Detects anomalies in the provided data.

        Args:
            data: The input data to analyze for anomalies. The type of data
                (e.g., string, list, DataFrame) depends on the specific implementation.

        Returns:
            The result of the detection process, typically a boolean indicator,
            a list of flagged rows, or a DataFrame with a detection column.

        Raises:
            NotImplementedError: If the method is not implemented by a subclass.
        """
        pass


class RepairStrategy(ABC):
    """
    Abstract base class for repair strategies.

    Repair strategies are used to address detected anomalies by normalizing,
    replacing, or removing the affected data.
    """

    @abstractmethod
    def repair(self, data):
        """
        Repairs anomalies in the provided data.

        Args:
            data: The input data with anomalies to repair. The type of data
                (e.g., string, list, DataFrame) depends on the specific implementation.

        Returns:
            The repaired data, typically in the same structure and type as the input.

        Raises:
            NotImplementedError: If the method is not implemented by a subclass.
        """
        pass
