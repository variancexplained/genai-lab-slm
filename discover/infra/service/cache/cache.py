#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/service/cache/cache.py                                              #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday September 14th 2024 08:30:37 pm                                            #
# Modified   : Thursday October 24th 2024 03:26:55 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Key Value Store Module"""
import logging
import os
import shelve
from typing import Any, Optional

from discover.infra.config.app import AppConfigReader
from discover.infra.service.cache.base import Cache
from discover.infra.utils.date_time.format import ThirdDateFormatter
from discover.infra.utils.file.compress import TarGzHandler

# ------------------------------------------------------------------------------------------------ #
tgz = TarGzHandler()
dt4mtr = ThirdDateFormatter()


# ------------------------------------------------------------------------------------------------ #
#                                  DISCOVER CACHE                                                  #
# ------------------------------------------------------------------------------------------------ #
class DiscoverCache(Cache):
    """
    DiscoverCache is a specialized cache system that handles key-value storage, retrieval, and management.
    It supports item registration, expiration, eviction, and data archiving. The cache uses a file-based
    registry and directories for persisting cache data.

    Args:
        config_reader_cls (type[ConfigReader], optional): Class responsible for reading configuration settings.
            Defaults to ConfigReader.
        io_cls (type[IOService], optional): Class responsible for handling input/output operations (e.g., file reading/writing).
            Defaults to IOService.

    """

    def __init__(
        self,
        config_reader_cls: type[AppConfigReader] = AppConfigReader,
    ) -> None:
        super().__init__(config_reader_cls=config_reader_cls)

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        self._validate_cache()

    def __len__(self) -> int:
        """Returns the number of keys in the cache registry."""
        with shelve.open(self._filepath) as kvs:
            return len(kvs)

    def __iter__(self):
        """Returns an iterator for iterating through keys in the cache registry."""
        self._kvs = shelve.open(self._filepath)
        self._iter_keys = iter(self._kvs.keys())
        return self

    def __next__(self) -> Any:
        """Returns the next key in the iteration."""
        try:
            key = next(self._iter_keys)
            return key
        except StopIteration:
            self._kvs.close()
            raise

    def add_item(self, key: str, data: Any, **kwargs) -> None:
        """
        Adds a new item to the cache, replacing it if the key already exists.

        If the key already exists, the existing item is evicted before adding the new item.

        Args:
            key (str): The key for the cache entry.
            data (Any): The value/data to be stored in the cache.

        Raises:
            FileExistsError: If the key already exists and cannot be evicted.
        """
        if self.exists(key=key):
            self.evict(key=key)
        self._admit(key=key, data=data, **kwargs)
        self._logger.debug(f"Added {key} to Cache.")

    def exists(self, key: str) -> bool:
        """
        Checks whether a key exists in the cache.

        Args:
            key (str): The key to check.

        Returns:
            bool: True if the key exists, False otherwise.

        Raises:
            FileNotFoundError: If the registry file is not found.
            Exception: If there is an unknown issue while reading the registry.
        """
        try:
            with shelve.open(self._filepath) as cache:
                return key in cache.keys()
        except FileNotFoundError as e:
            msg = f"Cache file not found.\n{e}"
            self._logger.exception(msg)
            raise
        except Exception as e:
            msg = f"Unknown error occurred while reading registry for cache.\n{e}"
            self._logger.exception(msg)
            raise

    def get_item(self, key: str) -> Any:
        """
        Retrieves an item from the cache based on the provided key.

        Args:
            key (str): The key for the cache entry.

        Returns:
            Any: The data associated with the key, or None if the key does not exist.
        """
        return self._read(key=key)

    def evict(self, key: str, **kwargs) -> None:
        """
        Evicts a cache entry, archives the associated data, and updates the cache registry.

        Args:
            key (str): The key to be evicted.
        """
        self._remove_item(key=key)
        msg = f"Key {key} has been evicted from the cache."
        self._logger.debug(msg)

    def reset(self, taskname: str) -> None:
        """
        Resets the cache by clearing all entries after a confirmation prompt.

        Args:
            taskname (str): Name of task for which the cache must be reset.

        Raises:
            Exception: If an error occurs during the reset operation.
        """

        confirmation = input(
            f"Are you sure you want to reset the cache for {taskname} in the {self._env} environment? This action cannot be undone. (yes/no): "
        )
        if confirmation.lower() != "yes":
            print("Reset operation aborted.")
            return
        try:
            with shelve.open(self._filepath) as kvs:
                for key in kvs.keys():
                    if (
                        taskname.lower() in key.lower()
                        and self._env.lower() in key.lower()
                    ):
                        del kvs[key]
            self._logger.info("KVS has been reset.")
        except Exception as e:
            msg = f"Exception occurred while resetting cache.\n{e}"
            self._logger.exception(msg)
            raise

    def _admit(self, key: str, data: Any, **kwargs) -> None:
        """
        Admits a new cache item by registering it and persisting its data.

        Args:
            key (str): The key to register and store data under.
            data (Any): The data to be stored.
        """
        try:
            with shelve.open(self._filepath) as cache:
                cache[key] = data
        except Exception as e:
            msg = f"Exception occurred while writing to cache.\n{e}"
            self._logger.exception(msg)
            raise

    def _read(self, key: str) -> Optional[Any]:
        """
        Reads an item from the cache.

        Args:
            key (str): The key for the cache entry.

        Returns:
            Any: Returns the data from cache.

        Raises:
            KeyError: If the key does not exist in the cache registry.
        """
        try:
            with shelve.open(self._filepath) as kvs:
                return kvs[key]
        except KeyError as ke:
            msg = f"No key {key} found in the cache.\n{ke}"
            self._logger.exception(msg)
            raise
        except Exception as e:
            msg = f"Unknown exception occurred while reading the cache.\n{e}"
            self._logger.exception(msg)
            raise

    def _remove_item(self, key: str) -> None:
        """
        Removes a cache entry's item.

        Args:
            key (str): The key to remove from cache.
        """
        try:
            with shelve.open(self._filepath) as kvs:
                del kvs[key]
        except KeyError as ke:
            msg = f"No key {key} found in the cache.\n{ke}"
            self._logger.warning(msg)

    def _validate_cache(self) -> None:
        """
        Ensures the cache exists and is accessible.

        This method ensures that:
        - The cache registry file exists and can be accessed.
        - The data and archive directories exist.
        """

        # Ensure the directories exist
        os.makedirs(os.path.dirname(self._filepath), exist_ok=True)
        try:
            with shelve.open(self._filepath):
                pass
        except FileNotFoundError as fe:
            self._logger.exception(
                f"Cache registry {self._filepath} could not be created.\n{fe}"
            )
            raise
        except Exception as e:
            self._logger.exception(
                f"Exception occurred while reading {self._filepath}.\n{e}"
            )
            raise
