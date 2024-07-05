#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/shared/persist/object/cache.py                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday June 5th 2024 05:40:02 am                                                 #
# Modified   : Thursday July 4th 2024 07:38:34 pm                                                  #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import logging
from functools import wraps
from typing import Any

from appinsight.shared.persist.object.kvs import KVS


# ------------------------------------------------------------------------------------------------ #
def cachenow(max_size=100, evict_size=5):
    def decorator(func):

        @wraps(func)
        def wrapper(self, *args, **kwargs):
            # Obtain the KVS (key/value store)
            department = "cache"
            name = "cachenow"
            kvs = KVS(department=department, name=name)
            # Generate a cache key based on class name, function name, and arguments
            if isinstance(func, property):
                class_name = type(self).__name__
                cache_key = f"{class_name}_{func.__name__}"
            else:
                class_name = type(self).__name__
                cache_key = f"{class_name}_{func.__name__}_{args}_{kwargs}"
            # Check if 'force' is in kwargs
            force = kwargs.pop("force", False)

            # Check if the result is already cached
            if kvs.exists(key=cache_key) and not force:
                # Return the cached value
                return kvs.read(key=cache_key)
            else:
                # Call the original function
                result = func(self, *args, **kwargs)
                # Cache the result
                kvs.create(key=cache_key, value=result)
                return result

        # Check if the function is a method of a class
        if isinstance(func, property):
            return property(wrapper)
        return wrapper

    return decorator


# ------------------------------------------------------------------------------------------------ #
#                                      CACHE MANAGER                                               #
# ------------------------------------------------------------------------------------------------ #
class Cache:
    """Represents a cache interface using a key-value store (KVS).

    This class provides methods to interact with a cache implemented using a key-value store.
    It delegates all cache operations to an underlying KVS instance.

    Args:
        name (str): The name of the cache instance.
        kvs_cls (type[KVS], optional): Class of the key-value store to use. Defaults to KVS.

    Properties:
        kvs (KVS): Exposes the underlying KVS instance to leverage as an iterator.

    Methods:
        add_item(key: Any, value: Any) -> None:
            Adds a key-value pair to the cache.

        exists(key: str) -> bool:
            Checks if a key exists in the cache.

        get_item(key: Any):
            Retrieves an item from the cache by its key.

        remove_item(key):
            Removes an item from the cache by its key.

        clear_cache():
            Clears all items from the cache.

    Examples:
        >>> cache = Cache(name='example_cache')
        >>> cache.add_item('key1', 'value1')
        >>> cache.exists('key1')
        True
        >>> cache.get_item('key1')
        'value1'
        >>> cache.remove_item('key1')
        >>> cache.clear_cache()

    """

    __department = "cache"

    def __init__(self, name: str, kvs_cls: type[KVS] = KVS):
        self._name = name
        self._kvs = kvs_cls(department=self.__department, name=name)
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @property
    def kvs(self) -> KVS:
        """Exposes the underlying KVS to allow callers to leverage KVS as an iterator."""
        return self._kvs

    def add_item(self, key: Any, value: Any) -> None:
        """Adds a key-value pair to the cache.

        Args:
            key (Any): The key to add to the cache.
            value (Any): The value associated with the key.
        """
        self._kvs.create(key=key, value=value)

    def exists(self, key: str) -> bool:
        """Returns True if the key exists in the cache, otherwise False.

        Args:
            key (str): The key to check for existence in the cache.

        Returns:
            bool: True if the key exists, False otherwise.
        """
        return self._kvs.exists(key=key)

    def get_item(self, key: Any):
        """Retrieves the value associated with the given key from the cache.

        Args:
            key (Any): The key whose associated value is to be retrieved.

        Returns:
            Any: The value associated with the key.
        """
        return self._kvs.read(key=key)

    def remove_item(self, key):
        """Removes the item associated with the given key from the cache.

        Args:
            key: The key of the item to be removed from the cache.
        """
        self._kvs.delete(key=key)

    def clear_cache(self):
        """Clears all items from the cache."""
        self._kvs.reset()
