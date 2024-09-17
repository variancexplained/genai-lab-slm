#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/domain/service/core/cache.py                                              #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday September 14th 2024 08:23:12 pm                                            #
# Modified   : Tuesday September 17th 2024 03:19:12 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import logging
from functools import wraps
from typing import Any

from discover.domain.service.core.data import find_dataframe, hash_dataframe
from discover.domain.value_objects.context import Context
from discover.domain.value_objects.file import KVSType
from discover.infra.storage.local.kvs import KVS


# ------------------------------------------------------------------------------------------------ #
def cachenow(func):
    """
    Caches the results of a function based on the input DataFrame.

    This decorator checks if the result of the decorated function is already cached based on a hash of the
    DataFrame passed as an argument. If the result is cached, it retrieves the value from the cache.
    If not, or if the `force` flag in the configuration is set to True, the function is executed, and
    its result is cached for future use.

    The cache key is generated using the function's fully qualified name and a dataset identifier created
    by hashing the input DataFrame.

    Parameters:
    -----------
    func : callable
        The function to be decorated.

    Returns:
    --------
    Any
        The result of the decorated function, either retrieved from cache or computed by executing the function.

    Example:
    --------
    @cachenow
    def process_data(self, df):
        # Function logic
        return processed_result

    Notes:
    ------
    - The cache is based on the hash of the input DataFrame and the function's fully qualified name.
    - If the 'force' flag in the configuration is set to True, the function is executed and the cache is updated.
    - If cache retrieval fails, the function is executed, and the result is cached.
    - Warnings are logged if there are issues with cache access or storage.
    """

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        run_task = False
        result = None

        # Find the dataframe among the args and kwargs
        df = find_dataframe(args, kwargs)
        # Create a dataset identifier as the hash for the dataframe.
        dataset_id = hash_dataframe(df=df)
        # Obtain a cache object
        cache = Cache(context=self.context, dataset_id=dataset_id)
        # Create a key from the class name.
        cache_key = self.__class__.__name__.lower()

        # Determine whether the task should be run based on configuration and cache
        run_task = self.config.force or not cache.exists(key=cache_key)

        # If not running the task, confirm results can be obtained from cache.
        if not run_task:
            try:
                result = cache.get_item(key=cache_key)
            except Exception as e:
                msg = f"Unable to obtain results for {cache_key} from the cache.\n{e}."
                logging.warning(msg)

        # If result is None because cache retrieval failed or force is True, run task and attempt to cache results.
        if result is None:
            result = func(self, *args, **kwargs)
            try:
                cache.add_item(key=cache_key, value=result)
            except Exception as e:
                msg = f"Unable to add result to cache {cache_key}.\n{e}"
                logging.warning(msg)

        return result

    return wrapper


# ------------------------------------------------------------------------------------------------ #
#                                      CACHE MANAGER                                               #
# ------------------------------------------------------------------------------------------------ #
class Cache:
    """
    A class representing a caching mechanism using a key-value store (KVS).

    This class provides a simple interface for interacting with a cache, allowing for adding,
    retrieving, checking existence, and removing items from the cache. It wraps around a KVS
    (Key-Value Store) instance and exposes relevant methods for cache operations.

    Attributes:
    -----------
    __KVS_TYPE : KVSType
        The type of KVS used for caching (fixed as KVSType.CACHE).
    _context : Context
        The pipeline context associated with the cache, providing metadata about the current stage and task.
    _dataset_id : str
        A unique identifier for the dataset associated with the cache.
    _kvs : KVS
        The underlying KVS instance used for cache operations.
    _logger : logging.Logger
        Logger instance for logging cache-related operations.

    Parameters:
    -----------
    context : Context
        The context object representing the current pipeline's execution phase, stage, and task.
    dataset_id : str
        A unique identifier for the dataset associated with the cache.
    kvs_cls : type[KVS], optional
        The class of the KVS to be used, by default KVS.

    Methods:
    --------
    kvs() -> KVS:
        Exposes the underlying KVS to allow callers to leverage KVS as an iterator.

    add_item(key: str, value: Any) -> None:
        Adds a key-value pair to the cache.

    exists(key: str) -> bool:
        Returns True if the key exists in the cache, otherwise False.

    get_item(key: Any) -> Any:
        Retrieves the value associated with the given key from the cache.

    remove_item(key: str) -> None:
        Removes the item associated with the given key from the cache.

    clear_cache() -> None:
        Clears all items from the cache.
    """

    __KVS_TYPE = KVSType.CACHE

    def __init__(self, context: Context, dataset_id: str, kvs_cls: type[KVS] = KVS):
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self._context = context
        self._dataset_id = dataset_id

        self._kvs = kvs_cls(
            kvs_type=self.__KVS_TYPE, context=context, dataset_id=dataset_id
        )

    def __len__(self) -> int:
        """Returns the number of items in the cache."""
        return len(self._kvs)

    @property
    def location(self) -> str:
        """
        Exposes the location of the underlying KVS

        Returns:
        --------
        str:
            The path to the underlying KVS instance.
        """
        return self._kvs.filepath

    def add_item(self, key: str, value: Any) -> None:
        """
        Adds a key-value pair to the cache.

        Args:
        -----
        key : str
            The key to add to the cache.
        value : Any
            The value associated with the key.
        """
        self._kvs.create(key=key, value=value)

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
        return self._kvs.exists(key=key)

    def get_item(self, key: str):
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
        return self._kvs.read(key=key)

    def remove_item(self, key: str) -> None:
        """
        Removes the item associated with the given key from the cache.

        Args:
        -----
        key : Any
            The key of the item to be removed from the cache.
        """
        self._kvs.remove(key=key)

    def clear_cache(self) -> None:
        """
        Clears all items from the cache.
        """
        self._kvs.reset()

    def delete_cache(self) -> None:
        """
        Deletes the cache file.
        """
        confirm = input("Deleting the cache is irreversable. Please confirm. [Yes/No]")
        if "yes" in confirm.lower():
            self._kvs.delete()
            self._logger.info(f"Cache at {self._kvs.filepath} has been deleted.")
        else:
            self._logger.info(f"Cache at {self._kvs.filepath} has been retained.")
