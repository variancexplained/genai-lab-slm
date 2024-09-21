#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/dynamics/optimization/utils.py                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday September 14th 2024 08:23:12 pm                                            #
# Modified   : Friday September 20th 2024 07:17:14 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import hashlib
from typing import Union

import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame

from discover.substance.entity.context.base import Context
from discover.substance.entity.task import Task


# ------------------------------------------------------------------------------------------------ #
def find_dataframe(args, kwargs) -> Union[pd.DataFrame, SparkDataFrame]:
    """
    This function is invoked by decorators that expect dataframes as arguments or keyword
    arguments. Searches for a pandas or Spark DataFrame in the positional and keyword arguments.

    Parameters:
    -----------
    args : tuple
        The positional arguments of the function.
    kwargs : dict
        The keyword arguments of the function.

    Returns:
    --------
    df : pandas.DataFrame or pyspark.sql.DataFrame or None
        Returns the found DataFrame, or None if no DataFrame is found.
    """
    df = None
    # Search through args first for pandas or Spark DataFrame
    for arg in args:
        if isinstance(arg, pd.DataFrame):
            df = arg
            break
        elif isinstance(arg, SparkDataFrame):
            df = arg
            break

    # If no DataFrame found in args, search through kwargs
    if df is None:
        for key, value in kwargs.items():
            if isinstance(value, pd.DataFrame):
                df = value
                break
            elif isinstance(value, SparkDataFrame):
                df = value
                break
    return df


# ------------------------------------------------------------------------------------------------ #
def find_task(args, kwargs) -> Context:
    """
    This function is invoked by decorators that expect a Task object as an argument or a keyword
    argument. Searches for a Task object in the positional and keyword arguments and returns
    the context attribute of the Task.

    Parameters:
    -----------
    args : tuple
        The positional arguments of the function.
    kwargs : dict
        The keyword arguments of the function.

    Returns:
    --------
    context : Context or None
        Returns the found Context, or None if no Context object is found.
    """
    task = None
    # Search through args first for a Task object
    for arg in args:
        if isinstance(arg, Task):
            task = arg
            break

    # If no context found in args, search through kwargs
    if task is None:
        for key, value in kwargs.items():
            if isinstance(value, Task):
                task = value
                break
    return task


# ------------------------------------------------------------------------------------------------ #
def get_dataset_cache_id(
    task: Task, df: Union[pd.DataFrame, SparkDataFrame], hash_length: int = 8
) -> str:
    """
    Generates a fixed-length hash based on the dimensions, schema, and optionally sampled data of a pandas or Spark DataFrame.

    This function creates a hash from the number of rows, number of columns, and schema of the DataFrame. Optionally,
    it includes sampled data from the DataFrame for increased uniqueness when content changes.

    Parameters:
    -----------
    df : Union[pd.DataFrame, pyspark.sql.DataFrame]
        The DataFrame for which to generate a hash. Can be either a pandas DataFrame or a Spark DataFrame.
    hash_length : int, optional
        The length of the hash to be generated (default is 8 characters).

    Returns:
    --------
    str
        The generated hash as a string of the specified length.

    Raises
    ------
    ValueError: On empty Dataframe

    Example:
    --------
    >>> import pandas as pd
    >>> df = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
    >>> hash_dataframe(df)
    '9a83f5ac'

    >>> from pyspark.sql import SparkSession
    >>> spark = SparkSession.builder.appName("example").getOrCreate()
    >>> sdf = spark.createDataFrame([(1, 2), (3, 4)], ["col1", "col2"])
    >>> hash_dataframe(sdf)
    'e3b0c442'
    """

    # Handle empty DataFrame case
    if isinstance(df, (pd.DataFrame, pd.core.frame.DataFrame)) and df.empty:
        raise ValueError("Encountered an empty dataframe")
    elif isinstance(df, SparkDataFrame) and df.count() == 0:
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

    # Combine the taskname, schema, and data for hashing
    dimensions_str = f"{taskname}-{num_rows}-{num_columns}-{schema_str}"

    # Generate a hash from the dimensions, schema, and sample data
    hasher = hashlib.blake2s(digest_size=hash_length)
    hasher.update(dimensions_str.encode("utf-8"))

    return hasher.hexdigest()
