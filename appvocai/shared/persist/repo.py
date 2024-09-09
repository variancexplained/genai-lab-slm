#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/shared/persist/repo.py                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday July 1st 2024 05:08:52 am                                                    #
# Modified   : Monday July 15th 2024 03:16:14 pm                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Repository Implementation Module"""
import logging
from abc import ABC, abstractmethod

import pandas as pd


# ------------------------------------------------------------------------------------------------ #
#                                         REPO                                                     #
# ------------------------------------------------------------------------------------------------ #
class Repo(ABC):
    """Encapsulates the repository interface"""

    @property
    @abstractmethod
    def registry(self) -> pd.DataFrame:
        """Returns repository registry"""

    @abstractmethod
    def add(self, **kwargs) -> None:
        """Adds an entity to the repository."""

    @abstractmethod
    def write(self, directory: str, filename: str, data: Any) -> None:
        """Writes data to file."""

    @abstractmethod
    def get_filepath(self, **kwargs) -> str:
        """Returns the filepath for the current environment."""

    @abstractmethod
    def exists(self, **kwargs) -> str:
        """Returns the existence of a ."""
