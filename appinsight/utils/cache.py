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
# Modified   : Thursday June 6th 2024 02:43:24 pm                                                  #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import os
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
class CacheManager:
    def __init__(self, shelf_file):
        self.shelf_file = shelf_file
        self._create_cache_directory()

    def _create_cache_directory(self):
        os.makedirs(os.path.dirname(self.shelf_file), exist_ok=True)

    def add_item(self, key, value):
        with shelve.open(self.shelf_file) as cache:
            cache[key] = value

    def get_item(self, key):
        with shelve.open(self.shelf_file) as cache:
            return cache.get(key)

    def remove_item(self, key):
        with shelve.open(self.shelf_file) as cache:
            if key in cache:
                del cache[key]

    def clear_cache(self):
        with shelve.open(self.shelf_file) as cache:
            cache.clear()

    def prune_cache(self, max_size):
        with shelve.open(self.shelf_file) as cache:
            if len(cache) > max_size:
                # Implement your eviction policy here
                # For example, remove the least recently used items
                pass
