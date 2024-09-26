#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/dal/base.py                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 05:39:55 pm                                              #
# Modified   : Wednesday September 25th 2024 04:51:22 pm                                           #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
# ------------------------------------------------------------------------------------------------ #
from abc import ABC, abstractmethod
from typing import Any


class DAO(ABC):
    """Abstract base class for data access objects."""

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
