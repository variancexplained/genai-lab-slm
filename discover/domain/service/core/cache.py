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
# Modified   : Tuesday September 17th 2024 10:04:22 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import logging
from abc import ABC, abstractmethod
from functools import wraps
from typing import Any

from discover.domain.service.core.data import find_dataframe, hash_dataframe
from discover.domain.value_objects.lifecycle import Stage
from discover.infra.config.reader import ConfigReader


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
        cache = Cache(stage=self.context.stage)
        # Determine whether the task should be run based on configuration and cache
        run_task = self.config.force or not cache.exists(key=dataset_id)

        # If not running the task, confirm results can be obtained from cache.
        if not run_task:
            try:
                result = cache.get_item(key=dataset_id)
            except Exception as e:
                msg = f"Unable to obtain results for {dataset_id} from the cache.\n{e}."
                logging.warning(msg)

        # If result is None because cache retrieval failed or force is True, run task and attempt to cache results.
        if result is None:
            result = func(self, *args, **kwargs)
            try:
                cache.add_item(key=dataset_id, value=result)
            except Exception as e:
                msg = f"Unable to add result to cache {dataset_id}.\n{e}"
                logging.warning(msg)

        return result

    return wrapper


# ------------------------------------------------------------------------------------------------ #
#                                      CACHE MANAGER                                               #
# ------------------------------------------------------------------------------------------------ #
class Cache(ABC):
    """"""

    def __init__(
        self, stage: Stage, config_reader_cls: type[ConfigReader] = ConfigReader
    ) -> None:
        self._stage = stage
        self._config = config_reader_cls().get_config(section="ops").cache

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

    @abstractmethod
    def check_expiry(self) -> None:
        """
        Performs cache evictions, serialization, and archiving according to the eviction policy

        """
