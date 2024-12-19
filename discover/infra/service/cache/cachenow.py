#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/service/cache/cachenow.py                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday September 14th 2024 08:23:12 pm                                            #
# Modified   : Thursday December 19th 2024 01:40:47 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import hashlib
import logging
from functools import wraps
from typing import Optional, Union

import pandas as pd
import pyspark

from discover.flow.task.base.base import Task
from discover.infra.config.app import AppConfigReader
from discover.infra.service.cache.cache import DiscoverCache
from discover.infra.utils.data.dataframe import find_dataframe

# ------------------------------------------------------------------------------------------------ #
reader = AppConfigReader()


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
        cache_id = get_dataset_cache_id(task=self, df=df)
        # Obtain a cache object
        cache = DiscoverCache()
        # Check the cache
        run_task = not cache.exists(key=cache_id) or self.force

        # If not running the task, confirm results can be obtained from cache.
        if not run_task:
            try:
                logging.debug(
                    f"Attempting to obtain the dataset {cache_id} from cache."
                )
                result = cache.get_item(key=cache_id)
                logging.debug(f"Successfully obtained dataset {cache_id} from cache.")
            except Exception as e:
                msg = f"Unable to obtain results for {cache_id} from the cache.\n{e}."
                logging.exception(msg)
                raise

        # If result is None because cache retrieval failed or force is True, run task and attempt to cache results.
        if result is None:
            result = func(self, *args, **kwargs)
            try:
                cache.add_item(key=cache_id, data=result)
                logging.debug(f"Added dataset {cache_id} to cache.")
            except Exception as e:
                msg = f"Unable to add result to cache {cache_id}.\n{e}"
                logging.exception(msg)
                raise

        return result

    return wrapper


# ------------------------------------------------------------------------------------------------ #
def get_dataset_cache_id(
    task: Task, df: Union[pd.DataFrame, pyspark.sql.DataFrame], hash_length: int = 8
) -> str:
    """
    Generates a fixed-length hash based on the dimensions, schema, and optionally sampled data of a pandas or Spark DataFrame.

    This function creates a hash from the number of rows, number of columns, and schema of the DataFrame. Optionally,
    it includes sampled data from the DataFrame for increased uniqueness when content changes.

    Args:
        task: An instance of the Task class.
        df : Union[pd.DataFrame, pyspark.sql.DataFrame]
            The DataFrame for which to generate a hash. Can be either a pandas DataFrame or a Spark DataFrame.
        hash_length : int, optional
            The length of the hash to be generated (default is 8 characters).

    Returns:
        str: The generated hash as a string of the specified length.

    """
    # Ensure cache is environment context-aware.
    env = reader.get_environment()

    # Handle empty DataFrame case
    if isinstance(df, (pd.DataFrame, pd.core.frame.DataFrame)) and df.empty:
        raise ValueError("Encountered an empty dataframe")
    elif isinstance(df, pyspark.sql.DataFrame) and df.count() == 0:
        raise ValueError("Encountered an empty dataframe")

    # Check if it's a pandas DataFrame
    if isinstance(df, (pd.DataFrame, pd.core.frame.DataFrame)):
        num_rows = len(df)
        num_columns = len(df.columns)
        schema_str = str(df.dtypes.values)
    else:
        num_rows = df.count()
        num_columns = len(df.columns)
        schema_str = df.schema.simpleString()

    # Get a name for the task based on its type.
    if isinstance(task, Task):
        taskname = task.__class__.__name__
    elif isinstance(task, type[Task]):
        taskname = task.__name__
    else:
        taskname = type(task).__name__

    # Get task specific key if available
    key = get_task_specific_key(task=task)

    # Combine the environment, taskname, dimensions, and schema
    cache_key = f"{env}-{taskname}-{num_rows}-{num_columns}-{schema_str}"
    if key:
        cache_key += f"-{key}"

    # Generate a hash from the dimensions, schema, and sample data
    hasher = hashlib.blake2s(digest_size=hash_length)
    hasher.update(cache_key.encode("utf-8"))

    return hasher.hexdigest()


def get_task_specific_key(task: Task) -> Optional[str]:
    """Returns any task specific key information that distinguishes this task execution.

    Any task specific keys should be defined here.

    Args:
        task: An instance of the Task class.

    Returns: str or None

    """
    try:
        return task.dqa_column
    except AttributeError:
        return None
    except Exception as e:
        msg = f"An unknown exception occurred in cachenow while obtaininng the task specific key.\n{e}"
        logging.exception(msg)
        raise
