#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/dynamics/ops/cachenow.py                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday September 14th 2024 08:23:12 pm                                            #
# Modified   : Friday September 20th 2024 05:23:08 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import logging
from functools import wraps

from discover.application.ops.utils import (
    find_dataframe,
    find_task,
    get_dataset_cache_id,
)
from discover.core.storage.local.cache import DiscoverCache


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
        # Search arguments for a Task object
        task = find_task(args, kwargs)
        # Create a dataset identifier as the hash for the dataframe.
        dataset_id = get_dataset_cache_id(task=task, df=df)
        # Obtain a cache object
        cache = DiscoverCache()
        # Check the cache
        run_task = not cache.exists(key=dataset_id)

        # If not running the task, confirm results can be obtained from cache.
        if not run_task:
            try:
                logging.debug(
                    f"Attempting to obtain the dataset {dataset_id} from cache."
                )
                result = cache.get_item(key=dataset_id)
                logging.debug(f"Successfully obtained dataset {dataset_id} from cache.")
            except Exception as e:
                msg = f"Unable to obtain results for {dataset_id} from the cache.\n{e}."
                logging.exception(msg)
                raise

        # If result is None because cache retrieval failed or force is True, run task and attempt to cache results.
        if result is None:
            result = func(self, *args, **kwargs)
            try:
                cache.add_item(key=dataset_id, data=result)
                logging.debug(f"Added dataset {dataset_id} to cache.")
            except Exception as e:
                msg = f"Unable to add result to cache {dataset_id}.\n{e}"
                logging.exception(msg)
                raise

        return result

    return wrapper
