#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/storage/local/cache.py                                              #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday September 14th 2024 08:30:37 pm                                            #
# Modified   : Sunday September 22nd 2024 04:26:08 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Key Value Store Module"""
import logging
import os
import shelve
import shutil
from datetime import datetime
from typing import Any, Optional, Union

import pandas as pd
import pyspark

from discover.dynamics.optimization.cache import Cache, CacheRegistration, CacheState
from discover.infra.config.reader import ConfigReader
from discover.infra.storage.local.io import IOService
from discover.infra.tools.date_time.format import ThirdDateFormatter
from discover.infra.tools.file.compress import TarGzHandler

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
        config_reader_cls: type[ConfigReader] = ConfigReader,
        io_cls: type[IOService] = IOService,
    ) -> None:
        super().__init__(config_reader_cls=config_reader_cls)
        self._io = io_cls()
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        self._validate_cache()

    def __len__(self) -> int:
        """Returns the number of keys in the cache registry."""
        with shelve.open(self._registry_filepath) as kvs:
            return len(kvs)

    def __iter__(self):
        """Returns an iterator for iterating through keys in the cache registry."""
        self._kvs = shelve.open(self._registry_filepath)
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
            with shelve.open(self._registry_filepath) as registry:
                return key in registry.keys()
        except FileNotFoundError as e:
            msg = f"Registry file not found for the cache.\n{e}"
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
        registration = self._read_registry(key=key)
        if registration:
            return self._read_data(registration=registration)
        return None

    def get_registration(self, key: str) -> CacheRegistration:
        """
        Retrieves the registration metadata for a cache entry.

        Args:
            key (str): The key for the cache entry.

        Returns:
            CacheRegistration: The metadata associated with the key.
        """
        return self._read_registry(key=key)

    def evict(self, key: str, **kwargs) -> None:
        """
        Evicts a cache entry, archives the associated data, and updates the cache registry.

        Args:
            key (str): The key to be evicted.
        """
        # Obtain the registration for the key
        registration = self._read_registry(key=key)
        # Get the archive location
        archive_filepath = self._get_archive_filepath(key)
        # Compress the object and archive
        tgz.compress_directory(
            directory_path=registration.filepath, tar_gz_path=archive_filepath
        )
        # Remove the data from cache data
        self._remove_data(registration=registration)
        # Update the registration
        registration.filepath = archive_filepath
        registration.state = CacheState.EVICTED
        registration.dt_modified = datetime.now()
        # Save the registration
        self._write_registration(registration=registration)

        msg = f"Key {key} has been evicted from the cache."
        self._logger.info(msg)

    def evict_expired(self) -> None:
        """
        Evicts all expired cache entries.

        Raises:
            Exception: If an unknown error occurs during the eviction process.
        """
        try:
            with shelve.open(self._registry_filepath) as kvs:
                for key, registration in kvs.items():
                    if registration.state == CacheState.EXPIRED:
                        self.evict(key=key)
        except Exception:
            msg = "Unknown error occurred in cache."
            self._logger.exception(msg)
            raise

    def reset(self) -> None:
        """
        Resets the cache by clearing all entries after a confirmation prompt.

        Raises:
            Exception: If an error occurs during the reset operation.
        """
        confirmation = input(
            "Are you sure you want to reset the cache? This action cannot be undone. (yes/no): "
        )
        if confirmation.lower() != "yes":
            print("Reset operation aborted.")
            return
        try:
            with shelve.open(self._registry_filepath) as kvs:
                for key, registration in kvs.items():
                    self._remove_data(registration=registration)
                    del kvs[key]
            self._logger.info("KVS has been reset.")
        except Exception as e:
            msg = f"Exception occurred while resetting cache.\n{e}"
            self._logger.exception(msg)
            raise

    def check_expiry(self) -> None:
        """
        Checks and updates the expiration status of cache items based on the TTL (Time-to-Live).

        If an item has expired, it updates its state to EXPIRED.

        Raises:
            FileNotFoundError: If the cache registry file is not found.
            Exception: If an error occurs during the expiry check process.
        """
        try:
            with shelve.open(self._registry_filepath) as kvs:
                for key, registration in kvs.items():
                    if (
                        datetime.now() - registration.dt_accessed
                    ).days >= self._config.ttl:
                        msg = f"Cache item key {key} in the cache has expired."
                        self._logger.info(msg)
                        registration.state = CacheState.EXPIRED
                        self._write_registration(registration=registration)
        except FileNotFoundError as e:
            msg = f"Registry for cache not found.\n{e}"
            self._logger.exception(msg)
            raise
        except Exception as e:
            msg = f"Exception occurred while reading cache registry.\n{e}"
            self._logger.exception(msg)
            raise

    def _admit(self, key: str, data: Any, **kwargs) -> None:
        """
        Admits a new cache item by registering it and persisting its data.

        Args:
            key (str): The key to register and store data under.
            data (Any): The data to be stored.
        """
        registration = self._register(key=key)
        self._write_registration(registration=registration)
        self._write_data(registration=registration, data=data, **kwargs)

    def _read_registry(self, key: str) -> Optional[CacheRegistration]:
        """
        Reads an entry's metadata (registration) from the cache registry.

        Args:
            key (str): The key for the cache entry.

        Returns:
            CacheRegistration: The metadata associated with the key, or None if the key does not exist.

        Raises:
            KeyError: If the key does not exist in the cache registry.
        """
        try:
            with shelve.open(self._registry_filepath) as kvs:
                registration = kvs[key]
                if registration.state == CacheState.EVICTED:
                    msg = f"Item key {key} was evicted from cache on {registration.dt_modified}"
                    self._logger.warning(msg)
                    return None
                return registration
        except KeyError as ke:
            msg = f"No key {key} found in the cache.\n{ke}"
            self._logger.exception(msg)
            raise

    def _write_registration(self, registration: CacheRegistration) -> None:
        """
        Writes or updates a cache entry's registration in the registry.

        Args:
            registration (CacheRegistration): The cache entry metadata to write.
        """
        try:
            with shelve.open(self._registry_filepath) as registry:
                registry[registration.key] = registration
        except FileNotFoundError as fnfe:
            msg = f"Cache Corruption Error. Registry not found for cache.\n{fnfe}"
            self._logger.exception(msg)
            raise
        except Exception as e:
            msg = f"Unknown error occurred while updating registry key: {registration.key} in the cache.\n{e}"
            self._logger.exception(msg)
            raise

    def _remove_registration(self, key: str) -> None:
        """
        Removes a cache entry's registration from the registry.

        Args:
            key (str): The key to remove from the registry.
        """
        try:
            with shelve.open(self._registry_filepath) as kvs:
                del kvs[key]
        except KeyError as ke:
            msg = f"No key {key} found in the cache.\n{ke}"
            self._logger.warning(msg)

    def _read_data(
        self, registration: CacheRegistration, **kwargs
    ) -> Union[pd.DataFrame, pyspark.sql.DataFrame]:
        """
        Reads and returns the data associated with a cache registration.

        Args:
            registration (CacheRegistration): The cache registration metadata.
            **kwargs: Additional parameters for the I/O operation.

        Returns:
            Union[pd.DataFrame, pyspark.sql.DataFrame]: The data read from the cache.

        Raises:
            FileNotFoundError: If the file associated with the key is not found.
            Exception: For other unknown errors.
        """
        try:
            data = self._io.read(filepath=registration.filepath, **kwargs)
            registration.dt_accessed = datetime.now()
            registration.n_accessed += 1
            self._write_registration(registration=registration)
            return data
        except FileNotFoundError as fnfe:
            msg = f"Cache Corruption Error. Item key: {registration.key} not found.\n{fnfe}"
            self._logger.exception(msg)
            raise
        except Exception as e:
            msg = f"Unknown error occurred while reading key: {registration.key} from the cache.\n{e}"
            self._logger.exception(msg)
            raise

    def _write_data(
        self,
        registration: CacheRegistration,
        data: Union[pd.DataFrame, pyspark.sql.DataFrame],
        **kwargs,
    ) -> None:
        """
        Writes the cache data to disk for a given registration.

        Args:
            registration (CacheRegistration): The cache registration metadata.
            data (Union[pd.DataFrame, pyspark.sql.DataFrame]): The data to be written.
            **kwargs: Additional parameters for the I/O operation.

        Raises:
            Exception: If an unknown error occurs while writing the data.
        """
        try:
            self._io.write(filepath=registration.filepath, data=data, **kwargs)
        except Exception as e:
            msg = f"Unknown error occurred while writing key: {registration.key} to the cache.\n{e}"
            self._logger.exception(msg)
            raise

    def _remove_data(self, registration: CacheRegistration) -> None:
        """
        Removes the data associated with a cache registration from disk.

        Args:
            registration (CacheRegistration): The cache registration metadata.
        """
        if os.path.isdir(registration.filepath):
            shutil.rmtree(registration.filepath)
        else:
            try:
                os.remove(registration.filepath)
            except Exception:
                pass

    def _get_data_filepath(self, key: str) -> str:
        """Constructs and returns the filepath where a cache item’s data is stored."""
        return os.path.join(self._data_directory, key) + ".parquet"

    def _get_archive_filepath(self, key: str) -> str:
        """Constructs and returns the filepath where a cache item’s archived data will be stored."""
        return os.path.join(self._archive_directory, key) + ".tar.gz"

    def _validate_cache(self) -> None:
        """
        Ensures the cache exists and is accessible.

        This method ensures that:
        - The cache registry file exists and can be accessed.
        - The data and archive directories exist.
        """

        # Ensure the directories exist
        os.makedirs(os.path.dirname(self._registry_filepath), exist_ok=True)
        try:
            with shelve.open(self._registry_filepath):
                pass
        except FileNotFoundError as fe:
            self._logger.exception(
                f"Cache registry {self._registry_filepath} could not be created.\n{fe}"
            )
            raise
        except Exception as e:
            self._logger.exception(
                f"Exception occurred while reading {self._registry_filepath}.\n{e}"
            )
            raise
        os.makedirs(self._data_directory, exist_ok=True)
        os.makedirs(self._archive_directory, exist_ok=True)

    def _register(self, key: str) -> CacheRegistration:
        """
        Registers a new cache item with metadata and an active state.

        Args:
            key (str): The key for the cache entry.

        Returns:
            CacheRegistration: The newly created cache registration.
        """
        now = datetime.now()
        return CacheRegistration(
            key=key,
            filepath=self._get_data_filepath(key=key),
            dt_added=now,
            dt_accessed=now,
            dt_modified=now,
            state=CacheState.ACTIVE,
        )
