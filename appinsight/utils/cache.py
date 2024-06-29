#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/utils/cache.py                                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday June 5th 2024 05:40:02 am                                                 #
# Modified   : Saturday June 29th 2024 04:46:30 pm                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import os
import logging
import shelve
from functools import wraps

from appinsight.utils.env import EnvManager


# ------------------------------------------------------------------------------------------------ #
def cachenow(max_size=100, evict_size=5):
    def decorator(func):

        @wraps(func)
        def wrapper(self, *args, **kwargs):
            # Get the current environment
            env = EnvManager().get_environment()
            # Set the shelve file
            shelf_file = f"cache/{env}/cache"
            # Generate a cache key based on class name, function name, and arguments
            if isinstance(func, property):
                class_name = type(self).__name__
                cache_key = f"{class_name}_{func.__name__}"
            else:
                class_name = type(self).__name__
                cache_key = f"{class_name}_{func.__name__}_{args}_{kwargs}"
            # Check if 'force' is in kwargs
            force = kwargs.pop("force", False)
            # Create a directory for the cache
            os.makedirs(os.path.dirname(shelf_file), exist_ok=True)
            # Open the shelve file
            with shelve.open(shelf_file) as cache:
                # Check if the result is already cached
                if cache_key in cache and not force:
                    # Move the cached item to the end (most recently used)
                    value = cache.pop(cache_key)
                    cache[cache_key] = value
                    return value
                else:
                    # Call the original function
                    result = func(self, *args, **kwargs)
                    # Cache the result
                    cache[cache_key] = result
                    # Check if eviction is needed
                    if max_size is not None and len(cache) > max_size:
                        # Remove the least recently used item
                        cache.popitem(last=False)
                    return result

        # Check if the function is a method of a class
        if isinstance(func, property):
            return property(wrapper)
        return wrapper

    return decorator


# ------------------------------------------------------------------------------------------------ #
#                                      CACHE MANAGER                                               #
# ------------------------------------------------------------------------------------------------ #
class CacheManager:
    def __init__(self, name: str, env_mgr_cls: type[EnvManager] = EnvManager):
        self._name = name
        self._env_mgr = env_mgr_cls()
        self._env = self._env_mgr.get_environment()
        self._shelf_file = os.path.join("cache", self._env, self._name)
        self._create_cache_directory()
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self._logger.debug(f"Instantiated the {name} cache manager.")

    def _create_cache_directory(self):
        os.makedirs(os.path.dirname(self._shelf_file), exist_ok=True)

    def add_item(self, key, value):
        try:
            with shelve.open(self._shelf_file) as cache:
                cache[key] = value
            self._logger.debug(f"Added key: {key} Value contains {len(value)} items.")
        except FileNotFoundError as fe:
            self._logger.exception(
                f"Shelve file {self._shelf_file} does not exist.\n{fe}"
            )
            raise
        except Exception as e:
            self._logger.exception(
                f"Exception occurred while adding item to cache.\n{e}"
            )
            raise

    def exists(self, key: str) -> bool:
        """Returns existence of key in cache

        Args:
            key (str): Key of item in cache.
        """
        try:
            with shelve.open(self._shelf_file) as cache:
                return key in cache
        except FileNotFoundError as fe:
            self._logger.exception(
                f"Shelve file {self._shelf_file} does not exist.\n{fe}"
            )
            raise
        except Exception as e:
            self._logger.exception(f"Exception occurred while reading cache.\n{e}")
            raise

    def get_item(self, key):
        try:
            with shelve.open(self._shelf_file) as cache:
                return cache.get(key)
        except FileNotFoundError as fe:
            self._logger.exception(
                f"Shelve file {self._shelf_file} does not exist.\n{fe}"
            )
            raise
        except Exception as e:
            self._logger.exception(f"Exception occurred while reading cache.\n{e}")
            raise

    def remove_item(self, key):
        try:
            with shelve.open(self._shelf_file) as cache:
                if key in cache:
                    del cache[key]
        except FileNotFoundError as fe:
            self._logger.exception(
                f"Shelve file {self._shelf_file} does not exist.\n{fe}"
            )
            raise
        except Exception as e:
            self._logger.exception(
                f"Exception occurred while removing item from cache.\n{e}"
            )
            raise

    def clear_cache(self):
        try:
            with shelve.open(self._shelf_file) as cache:
                for key in cache:
                    del cache[key]
        except FileNotFoundError as fe:
            self._logger.exception(
                f"Shelve file {self._shelf_file} does not exist.\n{fe}"
            )
            raise
        except Exception as e:
            self._logger.exception(f"Exception occurred while clearing cache.\n{e}")
            raise


# ------------------------------------------------------------------------------------------------ #
#                                      CACHE ITERATOR                                              #
# ------------------------------------------------------------------------------------------------ #
class CacheIterator:
    def __init__(self, name: str, env_mgr_cls: type[EnvManager] = EnvManager):
        self._name = name
        self._env_mgr = env_mgr_cls()
        self._env = self._env_mgr.get_environment()
        self._shelf_file = os.path.join("cache", self._env, self._name)
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self._logger.debug(f"Instantiated the {name} cache iterator.")

        self._index = 0
        self._keys = self._get_keys()

    def __iter__(self):
        return self

    def __next__(self):
        if self._index < len(self._keys):
            try:
                with shelve.open(self._shelf_file) as cache:
                    value = cache[self._keys[self._index]]
            except FileNotFoundError as fe:
                self._logger.exception(
                    f"Shelve file {self._shelf_file} does not exist.\n{fe}"
                )
                raise
            except Exception as e:
                self._logger.exception(
                    f"Exception occurred while reading from cache.\n{e}"
                )
                raise
            self._index += 1
            return value
        else:
            raise StopIteration

    def _get_keys(self) -> list:
        try:
            with shelve.open(self._shelf_file) as cache:
                return [key for key in cache]
        except FileNotFoundError as fe:
            self._logger.exception(
                f"Shelve file {self._shelf_file} does not exist.\n{fe}"
            )
            raise
        except Exception as e:
            self._logger.exception(
                f"Exception occurred while removing item from cache.\n{e}"
            )
            raise
