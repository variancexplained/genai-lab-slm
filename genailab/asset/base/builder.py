#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab                                                                           #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/asset/base/builder.py                                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday December 18th 2024 03:01:02 pm                                            #
# Modified   : Saturday February 8th 2025 10:43:32 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Base Module for the Asset Dimension"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import List

from genailab.asset.base.asset import Asset


# ------------------------------------------------------------------------------------------------ #
#                                   ASSET BUILDER                                                  #
# ------------------------------------------------------------------------------------------------ #
class AssetBuilder(ABC):
    """
    Abstract base class for building assets with phases, stages, and persistence
    configurations.
    """

    def __init__(self):
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @abstractmethod
    def reset(self) -> None:
        """
        Resets the builder to be ready to construct another Dataset object.
        """
        pass

    @abstractmethod
    def build(self) -> Asset:
        """
        Builds and returns the final Dataset object based on the provided configurations.

        Returns:
            Dataset: The fully constructed dataset.
        """
        pass

    def _report_validation_errors(self, errors: List[str]) -> None:
        errors = "\n".join(errors)
        self._logger.error(errors)
        raise ValueError(errors)
