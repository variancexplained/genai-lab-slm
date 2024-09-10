#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/domain/service/repo.py                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday September 9th 2024 03:00:19 pm                                               #
# Modified   : Monday September 9th 2024 07:46:50 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Persistence base class module"""
from abc import ABC, abstractmethod
from typing import Any, Dict


# ------------------------------------------------------------------------------------------------ #
class Repo(ABC):
    """Persistence base class."""

    @abstractmethod
    def add(self, *args: Any, **kwargs: Dict[str, Any]) -> None:
        """Persists data."""

    @abstractmethod
    def get(self, *args: Any, **kwargs: Dict[str, Any]) -> Any:
        """Reads an data from persistence."""

    @abstractmethod
    def remove(self, *args: Any, **kwargs: Dict[str, Any]) -> None:
        """Deletes data from persistence."""

    @abstractmethod
    def exists(self, *args: Any, **kwargs: Dict[str, Any]) -> bool:
        """Deletes data from persistence."""
