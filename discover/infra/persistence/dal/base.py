#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/persistence/dal/base.py                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 05:39:55 pm                                              #
# Modified   : Wednesday December 18th 2024 06:22:49 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data Access Layer Base Module"""

from abc import ABC, abstractmethod
from typing import Any


# ------------------------------------------------------------------------------------------------ #
class DAL(ABC):
    """Abstract base class for the data access layer."""

    @abstractmethod
    def create(self, *args, **kwargs) -> None:
        """Adds data to an underlying storage mechanism.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """

    @abstractmethod
    def read(self, *args, **kwargs) -> Any:
        """Reads data from an underlying storage mechanism.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            Any: The data read from the storage.
        """

    @abstractmethod
    def delete(self, *args, **kwargs) -> None:
        """Deletes data from an underlying storage mechanism.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """

    @abstractmethod
    def exists(self, *args, **kwargs) -> bool:
        """Determines existence of an alement

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """


# ------------------------------------------------------------------------------------------------ #
class LocationService(ABC):
    """
    Abstract base class for defining a location service that provides file path generation.
    This class is intended to be inherited by concrete implementations that define how file
    paths are constructed based on specific parameters.
    """

    @abstractmethod
    def get_filepath(self, **kwargs) -> str:
        """
        Abstract method to generate a file path

        Args:
            **kwargs: Arbitrary keyword arguments.

        Returns:
            str: The generated file path as a string.
        """
        pass
