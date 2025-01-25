#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailabslm/flow/dataprep/quality/strategy/base.py                                 #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 21st 2024 12:34:06 am                                             #
# Modified   : Saturday January 25th 2025 04:41:09 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Strategy Base Class Module"""
from abc import ABC, abstractmethod

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
