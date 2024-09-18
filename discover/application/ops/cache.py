#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/application/ops/cache.py                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday September 14th 2024 08:23:12 pm                                            #
# Modified   : Wednesday September 18th 2024 03:24:29 pm                                           #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from functools import wraps
from typing import Any, Optional

from discover.application.ops.utils import find_context, find_dataframe, hash_dataframe
from discover.core.data import DataClass
from discover.domain.value_objects.lifecycle import Stage
from discover.infra.config.reader import ConfigReader
from discover.infra.storage.local.cache import DiscoverCache


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
        # Search arguments for a Task object and return its context
        context = find_context(args, kwargs)
        # Create a dataset identifier as the hash for the dataframe.
        dataset_id = hash_dataframe(df=df)
        # Obtain a cache object
        cache = DiscoverCache(stage=context.stage)
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
                cache.add_item(key=dataset_id, data=result)
            except Exception as e:
                msg = f"Unable to add result to cache {dataset_id}.\n{e}"
                logging.warning(msg)

        return result

    return wrapper


# ------------------------------------------------------------------------------------------------ #
class CacheState(Enum):
    """
    Enum representing the state of a cached item.

    This enumeration is used to track the current state of an item in the cache.
    It helps in managing cache lifecycle events like expiration and eviction.

    Attributes:
    -----------
    ACTIVE : CacheState
        Indicates that the cached item is currently active and available.

    EXPIRED : CacheState
        Indicates that the cached item has expired and is no longer valid.

    EVICTED : CacheState
        Indicates that the cached item has been evicted from the cache, either due to cache size limits
        or other eviction policies.
    """

    ACTIVE = "Active"
    EXPIRED = "Expired"
    EVICTED = "Evicted"


# ------------------------------------------------------------------------------------------------ #
@dataclass
class CacheRegistration(DataClass):
    """
    A data class representing a registration entry in the cache system.

    This class tracks the metadata associated with a cached item, including its
    key, file location, timestamps for when it was added, accessed, and modified,
    as well as the state of the item (e.g., active, expired, evicted) and the
    number of times it has been accessed.

    Attributes:
    -----------
    stage : Stage
        The stage of the pipeline or process for which this cache entry is relevant.

    key : str
        The unique key identifying the cached item.

    filepath : str
        The file path where the cached item is stored.

    dt_added : datetime
        The timestamp when the item was added to the cache.

    dt_accessed : Optional[datetime]
        The timestamp of the last time the cached item was accessed (cache hit).
        Defaults to None if the item has not yet been accessed.

    dt_modified : Optional[datetime]
        The timestamp of the last modification to the cache entry, such as
        when the state changes due to expiration or eviction. Defaults to None.

    n_accessed : int
        The number of times the cached item has been accessed. Defaults to 0.

    state : CacheState
        The current state of the cached item (e.g., active, expired, evicted).
        Defaults to CacheState.ACTIVE.
    """

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
    _stage : Stage
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
