#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/domain/repo.py                                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday July 1st 2024 05:01:39 am                                                    #
# Modified   : Tuesday July 2nd 2024 03:27:28 am                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Repo Interface Module"""
from abc import ABC, abstractmethod

import pandas as pd

from appinsight.domain.base import Entity


# ------------------------------------------------------------------------------------------------ #
class Repo(ABC):
    """Abstract base class defining the interface for repositories."""

    @property
    @abstractmethod
    def registry(self) -> pd.DataFrame:
        """Returns a DataFrame of repository contents."""

    @abstractmethod
    def add(self, entity: Entity) -> None:
        """Adds an entity to the repository."""

    @abstractmethod
    def get(self, oid: int) -> Entity:
        """Returns an item from the repository."""

    @abstractmethod
    def remove(self, *args, **kwargs) -> None:
        """Removes zero, one or more items from the repository."""
