#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/storage/local/kvs.py                                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday September 14th 2024 08:30:37 pm                                            #
# Modified   : Tuesday September 17th 2024 01:52:41 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Key Value Store Module"""
import logging
import os
import shelve
import shutil
from typing import Any

from discover.domain.value_objects.context import Context
from discover.domain.value_objects.file import KVSType
from discover.infra.config.reader import ConfigReader
from discover.infra.storage.local.file_io import IOService


# ------------------------------------------------------------------------------------------------ #
class KVS:
    """Manages access to key/value (section) store for the project.

    The KVS is organized by section and name. This class expects that the
    environment configuration file has specified the location of the section
    and name under the 'kvs' key in the configuration files.

    Args:
        section (str): The section can be 'cache', 'repo', etc.
        name (str): The name of the KVS within the section.
        kvs_type (KVSType): The name of the key value store for which
            this class is being instantiated.
        config_reader_cls (type[EnvManager], optional): Environment manager class.
        io_cls (type[IOService], optional): IO service class.
    """

    def __init__(
        self,
        kvs_type: KVSType,
        context: Context,
        dataset_id: str,
        config_reader_cls: type[ConfigReader] = ConfigReader,
        io_cls: type[IOService] = IOService,
    ) -> None:
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self._kvs_type = kvs_type
        self._context = context
        self._dataset_id = dataset_id

        self._config_reader = config_reader_cls()
        self._io = io_cls()

        self._kvs_file = self._get_filepath(
            kvs_type=kvs_type, context=context, dataset_id=dataset_id
        )
        self._validate_kvs()

    def __len__(self) -> int:
        """Returns the number of keys in the KVS."""
        with shelve.open(self._kvs_file) as kvs:
            return len(kvs)

    def __iter__(self):
        """Returns an iterator for iterating through keys in the KVS."""
        self._kvs = shelve.open(self._kvs_file)
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

    @property
    def filepath(self) -> str:
        """Returns the filepath of the cache."""
        return self._kvs_file

    def create(self, key: str, value: Any) -> None:
        """Persists the object in the KVS.

        Args:
            key (str): Key for the entry.
            value (Any): Value to be stored.

        Raises:
            FileExistsError: If the key already exists.
        """
        if self.exists(key):
            msg = f"Cannot create {key} as it already exists."
            self._logger.exception(msg)
            raise FileExistsError(msg)

        with shelve.open(self._kvs_file) as kvs:
            kvs[key] = value
        self._logger.debug(f"Added {key} to KVS.")

    def read(self, key: str) -> Any:
        """Reads an object from the KVS.

        Args:
            key (str): Key for the entry.

        Returns:
            Any: The value associated with the key.

        Raises:
            KeyError: If the key does not exist.
        """
        try:
            with shelve.open(self._kvs_file) as kvs:
                return kvs[key]
        except KeyError as ke:
            msg = f"No key {key} found.\n{ke}"
            self._logger.exception(msg)
            raise

    def remove(self, key: str) -> None:
        """Deletes an object from the KVS.

        Args:
            key (str): Key for the entry.

        Raises:
            KeyError: If the key does not exist.
        """
        try:
            with shelve.open(self._kvs_file) as kvs:
                del kvs[key]
        except KeyError as ke:
            msg = f"No key {key} found.\n{ke}"
            self._logger.warning(msg)

    def exists(self, key: str) -> bool:
        """Checks if a key exists in the KVS.

        Args:
            key (str): Key for the entry.

        Returns:
            bool: True if the key exists, False otherwise.
        """
        try:
            with shelve.open(self._kvs_file) as kvs:
                return key in kvs
        except FileNotFoundError as e:
            msg = f"Unable to find kvs file: {self._kvs_file}.\n{e}"
            self._logger.exception(msg)
            raise
        except Exception as e:
            msg = f"Unknown exception occurred while reading {self._kvs_file}.\n{e}"
            raise

    def delete(self) -> None:
        """Deletes the cache file."""
        shutil.rmtree(self._kvs_file, ignore_errors=True)

    def reset(self) -> None:
        """Resets the KVS by clearing all entries.

        Raises:
            Exception: For any issues during the reset operation.
        """
        confirmation = input(
            f"Are you sure you want to reset the KVS at '{self._kvs_file}'? This action cannot be undone. (yes/no): "
        )
        if confirmation.lower() != "yes":
            print("Reset operation aborted.")
            return

        try:
            with shelve.open(self._kvs_file) as kvs:
                for key in list(kvs.keys()):
                    del kvs[key]
            self._logger.info("KVS has been reset.")
        except Exception as e:
            msg = f"Exception occurred while resetting {self._kvs_file}.\n{e}"
            self._logger.exception(msg)
            raise

    def _get_filepath(
        self, kvs_type: KVSType, context: Context, dataset_id: str
    ) -> str:
        """ """
        basedir = self._config_reader.get_config(section="ops", namespace=False)
        filepath = os.path.join(
            basedir,  # i.e. ops/dev
            kvs_type.value.lower(),  # i.e. cache
            context.phase.value.lower(),  # i.e. data_prep
            context.stage.value.lower(),  # i.e. dqa
            dataset_id.lower(),  # i.e. some has value
        )
        return filepath

    def _validate_kvs(self) -> None:
        """Validates the KVS is accessible.

        Ensures the directory exists in case this process creates the kvs file.

        Raises:
            FileNotFoundError: If the KVS file does not exist.
            Exception: For other issues accessing the KVS file.
        """
        os.makedirs(os.path.dirname(self._kvs_file), exist_ok=True)
        try:
            with shelve.open(self._kvs_file):
                return
        except FileNotFoundError as fe:
            self._logger.exception(f"KVS file {self._kvs_file} does not exist.\n{fe}")
            raise
        except Exception as e:
            self._logger.exception(
                f"Exception occurred while reading {self._kvs_file}.\n{e}"
            )
            raise
