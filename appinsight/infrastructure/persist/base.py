#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/infrastructure/persist/base.py                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday June 30th 2024 09:57:47 pm                                                   #
# Modified   : Sunday June 30th 2024 10:01:38 pm                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Persistence base class module"""
from abc import ABC, abstractmethod
from typing import Any


# ------------------------------------------------------------------------------------------------ #
class Persistence(ABC):
    """Persistence base class."""

    @abstractmethod
    def create(self, *args, **kwargs) -> None:
        """Persists data."""

    @abstractmethod
    def read(self, *args, **kwargs) -> Any:
        """Reads an data from persistence."""

    @abstractmethod
    def delete(self, *args, **kwargs) -> None:
        """Deletes data from persistence."""

    @abstractmethod
    def list(self, *args, **kwargs) -> None:
        """Lists data in persistence."""
