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
# Modified   : Tuesday September 17th 2024 10:58:16 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Key Value Store Module"""
import logging
import os
import shelve
import shutil
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional, Union

import pandas as pd
import pyspark

from discover.core.data import DataClass
from discover.core.date_time import ThirdDateFormatter
from discover.domain.service.core.cache import Cache
from discover.domain.value_objects.cache import CacheState
from discover.domain.value_objects.lifecycle import Stage
from discover.infra.config.reader import ConfigReader
from discover.infra.storage.local.file_io import IOService
from discover.infra.utils.file_utils.compress import TarGzHandler

# ------------------------------------------------------------------------------------------------ #
tgz = TarGzHandler()
dt4mtr = ThirdDateFormatter()


# ------------------------------------------------------------------------------------------------ #
@dataclass
class CacheRegistration(DataClass):
    stage: Stage
    key: str
    filepath: str
    dt_added: datetime
    dt_accessed: Optional[datetime] = None  # Datetime for cache hits.
    dt_modified: Optional[datetime] = (
        None  # This is for state changes, i.e. expiration, eviction.
    )
    n_accessed: int = 0
    state: CacheState = CacheState.ACTIVE


# ------------------------------------------------------------------------------------------------ #
class DiscoverCache(Cache):
    """ """

    def __init__(
        self,
        stage: Stage,
        config_reader_cls: type[ConfigReader] = ConfigReader,
        io_cls: type[IOService] = IOService,
    ) -> None:
        super().__init__(stage=stage, config_reader_cls=config_reader_cls)
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self._io = io_cls()

        # Establish cache file structure
        self._registry_filepath = os.path.join(self._config.basedir, "registry")
        self._data_directory = os.path.join(self._config.basedir, "data")
        self._archive_directory = os.path.join(self._config.basedir, "archive")

        self._validate_cache()

    def __len__(self) -> int:
        """Returns the number of keys in the Cache."""
        with shelve.open(self._registry_filepath) as kvs:
            return len(kvs)

    def __iter__(self):
        """Returns an iterator for iterating through keys in the KVS."""
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
        """Persists the object in the Cache.

        If it already exists, the user is prompted to confirm replacement.

        Args:
            key (str): Key for the entry.
            value (Any): Value to be stored.

        Raises:
            FileExistsError: If the key already exists.
        """

        if self.exists(key=key):
            self.evict(key=key)

        self._admit(key=key, data=data, **kwargs)
        self._logger.debug(f"Added {key} to {self._stage.description} Cache.")

    def exists(self, key: str) -> bool:
        try:
            with shelve.open(self._registry_filepath) as registry:
                return key in registry.keys()
        except FileNotFoundError as e:
            msg = (
                f"Registry file not found for the {self._stage.description} cache.\n{e}"
            )
            self._logger.exception(msg)
            raise
        except Exception as e:
            msg = f"Unknown exception occurred while reading registry for the {self._stage.description} cache.\n{e}"
            self._logger.exception(msg)
            raise

    def get_item(self, key: str) -> Any:
        registration = self._read_registry(key=key)
        if registration:
            return self._read_data(registration=registration)
        else:
            return None

    def get_registration(self, key: str) -> CacheRegistration:
        return self._read_registry(key=key)

    def evict(self, key: str, **kwargs) -> None:
        registration = self._read_registry(key=key)
        archive_filepath = self._get_archive_filepath(key)
        tgz.compress_directory(
            directory_path=registration.filepath, tar_gz_path=archive_filepath
        )
        self._remove_data(registration=registration)
        registration.filepath = archive_filepath
        registration.state = CacheState.EVICTED
        registration.dt_modified = datetime.now()
        self._write_registration(registration=registration)

        msg = f"Key {key} has been evicted from the {self._stage.description} cache."
        self._logger.info(msg)

    def evict_expired(self) -> None:
        try:
            with shelve.open(self._registry_filepath) as kvs:
                for key, registration in kvs.items():
                    if registration.state == CacheState.EXPIRED:
                        self.evict(key=key)
        except Exception:
            msg = f"Unknown error occurred in {self._stage.description} cache."
            self._logger.exception(msg)
            raise

    def reset(self) -> None:
        """Resets the KVS by clearing all entries.

        Raises:
            Exception: For any issues during the reset operation.
        """
        confirmation = input(
            f"Are you sure you want to reset the {self._stage.description} cache? This action cannot be undone. (yes/no): "
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
            msg = f"Exception occurred while resetting {self._stage.description} cache.\n{e}"
            self._logger.exception(msg)
            raise

    def check_expiry(self) -> None:
        try:
            with shelve.open(self._registry_filepath) as kvs:
                for key, registration in kvs.items():
                    if (
                        datetime.now() - registration.dt_accessed
                    ).days >= self._config.ttl:
                        msg = f"Cache item key {key} in the {self._stage.description} cache has expired."
                        self._logger.info(msg)
                        registration.state = CacheState.EXPIRED
                        self._write_registration(registration=registration)
        except FileNotFoundError as e:
            msg = f"Registry for {self._stage.description} cache not found.\n{e}"
            self._logger.exception(msg)
            raise
        except Exception as e:
            msg = f"Exception occurred while reading {self._stage.description} cache registry.\n{e}"
            self._logger.exception(msg)
            raise

    def _admit(self, key: str, data: Any, **kwargs) -> None:
        registration = self._register(key=key)
        self._write_registration(registration=registration)
        self._write_data(registration=registration, data=data, **kwargs)

    def _read_registry(self, key: str) -> Optional[CacheRegistration]:
        """Reads an object from the Cache.

        Args:
            key (str): Key for the entry.

        Returns:
            Any: The value associated with the key.

        Raises:
            KeyError: If the key does not exist.
        """
        try:
            with shelve.open(self._registry_filepath) as kvs:
                registration = kvs[key]
                if registration.state == CacheState.EVICTED:
                    msg = f"Item key {key} was evicted from {self._stage.description} cache on {dt4mtr.to_HTTP_format(registration.dt_modified)}"
                    self._logger.warning(msg)
                    return None
                else:
                    return registration

        except KeyError as ke:
            msg = f"No key {key} found in the {self._stage.description} cache.\n{ke}"
            self._logger.exception(msg)
            raise

    def _write_registration(self, registration: CacheRegistration) -> None:
        try:
            with shelve.open(self._registry_filepath) as registry:
                registry[registration.key] = registration
        except FileNotFoundError as fnfe:
            msg = f"Cache Corruption Error. Registery not found for {self._stage.description} cache.\n{fnfe}"
            self._logger.exception(msg)
            raise
        except Exception as e:
            msg = f"Unknown error occured while updating registry key: {registration.key} in the {self._stage.description} cache.\n{e}"
            self._logger.exception(msg)
            raise

    def _remove_registration(self, key: str) -> None:
        try:
            with shelve.open(self._registry_filepath) as kvs:
                del kvs[key]
        except KeyError as ke:
            msg = f"No key {key} found in the {self._stage.description} cache.\n{ke}"
            self._logger.warning(msg)

    def _read_data(
        self, registration: CacheRegistration, **kwargs
    ) -> Union[pd.DataFrame, pyspark.sql.DataFrame]:
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
            msg = f"Unknown error occured while reading key: {registration.key} from the cache.\n{e}"
            self._logger.exception(msg)
            raise

    def _write_data(
        self,
        registration: CacheRegistration,
        data: Union[pd.DataFrame, pyspark.sql.DataFrame],
        **kwargs,
    ) -> None:
        try:
            self._io.write(filepath=registration.filepath, data=data, **kwargs)
        except Exception as e:
            msg = f"Unknown error occured while writing key: {registration.key} to the {self._stage.description} cache.\n{e}"
            self._logger.exception(msg)
            raise

    def _remove_data(self, registration: CacheRegistration) -> str:
        if os.path.isdir(registration.filepath):
            shutil.rmtree(registration.filepath)
        else:
            try:
                os.remove(registration.filepath)
            except Exception:
                pass

    def _get_data_filepath(self, key: str) -> str:
        """ """
        return os.path.join(self._data_directory, key) + ""

    def _get_archive_filepath(self, key: str) -> str:
        """ """
        return os.path.join(self._archive_directory, key) + ".tar.gz"

    def _validate_cache(self) -> None:
        """Ensures cache exists and is accessible.

        Cache is comprised of:
        - CacheRegistry
        - Data Directory
        - Archive Directory

        This method ensures that these components exist and
        are accessible.
        """
        # Cache Registry
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

        # Data Directory
        os.makedirs(self._data_directory, exist_ok=True)
        # Archive Directory
        os.makedirs(self._archive_directory, exist_ok=True)

    def _register(self, key: str) -> CacheRegistration:
        now = datetime.now()
        return CacheRegistration(
            stage=self._stage,
            key=key,
            filepath=self._get_data_filepath(key=key),
            dt_added=now,
            dt_accessed=now,
            dt_modified=now,
            state=CacheState.ACTIVE,
        )
