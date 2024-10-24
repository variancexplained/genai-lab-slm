#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/service/cache/base.py                                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday September 14th 2024 08:23:12 pm                                            #
# Modified   : Thursday October 24th 2024 02:31:44 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from abc import ABC, abstractmethod
from typing import Any

from discover.infra.config.app import AppConfigReader


# ------------------------------------------------------------------------------------------------ #
#                                        CACHE                                                     #
# ------------------------------------------------------------------------------------------------ #
class Cache(ABC):
    """
    Abstract Base Class for managing cache operations.

    This class defines the core operations for a caching mechanism, including adding items,
    checking for existence, retrieving items, and managing expiration policies. It requires
    subclasses to implement the basic functionalities necessary for interacting with a cache,
    such as counting the number of items, adding new items, and determining if a key exists.

    Attributes:
    -----------
    _stage: StageDef
        The stage of the pipeline or process for which this cache is used.
    _config : Config
        Configuration settings related to caching, typically retrieved from a configuration file.

    Methods:
    --------
    __len__() -> int:
        Returns the number of items in the cache.

    add_item(key: str, data: Any) -> None:
        Adds an item to the cache.

    exists(key: str) -> bool:
        Checks if a key exists in the cache.

    get_item(key: str) -> Any:
        Retrieves the value associated with a given key from the cache.

    check_expiry() -> None:
        Handles cache expiry, including eviction, serialization, and archiving according to the
        implemented eviction policy.
    """

    def __init__(
        self, config_reader_cls: type[AppConfigReader] = AppConfigReader
    ) -> None:

        self._filepath = config_reader_cls().get_config(section="ops").cache
        self._env = config_reader_cls().get_environment()

    @abstractmethod
    def __len__(self) -> int:
        """Returns the number of items in the cache."""

    @abstractmethod
    def add_item(self, key: str, data: Any) -> None:
        """
        Adds an item to he cache.

        Args:
        -----
        key : str
            The key to add to the cache.
        value : Any
            The value associated with the key.
        """

    @abstractmethod
    def exists(self, key: str) -> bool:
        """
        Returns True if the key exists in the cache, otherwise False.

        Args:
        -----
        key : str
            The key to check for existence in the cache.

        Returns:
        --------
        bool:
            True if the key exists, False otherwise.
        """

    @abstractmethod
    def get_item(self, key: str) -> Any:
        """
        Retrieves the value associated with the given key from the cache.

        Args:
        -----
        key : Any
            The key whose associated value is to be retrieved.

        Returns:
        --------
        Any:
            The value associated with the key.
        """
