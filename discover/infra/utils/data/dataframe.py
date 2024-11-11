#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/utils/data/dataframe.py                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday September 20th 2024 04:35:45 pm                                              #
# Modified   : Sunday November 10th 2024 07:12:20 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""DataFrame Utility Module"""

import math
from enum import Enum
from typing import Tuple, Union

import pandas as pd
import psutil
from pyspark.sql import DataFrame as SparkDataFrame


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
        if isinstance(arg, (pd.DataFrame, pd.core.frame.DataFrame)):
            df = arg
            break
        elif isinstance(arg, SparkDataFrame):
            df = arg
            break

    # If no DataFrame found in args, search through kwargs
    if df is None:
        for key, value in kwargs.items():
            if isinstance(value, (pd.DataFrame, pd.core.frame.DataFrame)):
                df = value
                break
            elif isinstance(value, SparkDataFrame):
                df = value
                break
    return df


# ------------------------------------------------------------------------------------------------ #
def split_dataframe(data, n):
    """
    Split the DataFrame into n+1 chunks where the last chunk has len(data) % n rows.

    Args:
        data (pd.DataFrame): The DataFrame to be split.
        n (int): The number of chunks to split the DataFrame into.

    Returns:
        List[pd.DataFrame]: A list of DataFrame chunks.
    """
    chunk_size = len(data) // n
    remainder = len(data) % n

    chunks = [data.iloc[i * chunk_size : (i + 1) * chunk_size] for i in range(n)]

    if remainder > 0:
        chunks.append(data.iloc[n * chunk_size :])

    return chunks


# ------------------------------------------------------------------------------------------------ #
class DatasetSizeThreshold(Enum):
    """Manages the relationship between maximum dataset size and partition size."""

    SMALL = (10 * 1024**3, 256 * 1024**2)  # < 10 GB, 256 MB partitions
    MEDIUM = (100 * 1024**3, 512 * 1024**2)  # 10 GB - 100 GB, 512 MB partitions
    LARGE = (1024 * 1024**3, 768 * 1024**2)  # 100 GB - 1 TB, 768 MB partitions
    VERY_LARGE = (float("inf"), 1024 * 1024**2)  # > 1 TB, 1 GB partitions

    def __init__(self, max_size, partition_size):
        self.max_size = max_size
        self.partition_size = partition_size

    @classmethod
    def get_partition_size(cls, df_size: float):
        """Return the appropriate enum member based on the size of the dataset."""

        # Iterate over the enum members to find the appropriate one
        for threshold in cls:
            if df_size <= threshold.max_size:
                return threshold


# ------------------------------------------------------------------------------------------------ #
class Optimizer:
    def compute_partitions(
        self, df: pd.DataFrame, adjust: float = 1.0
    ) -> Tuple[int, float]:

        # Obtain dataframe size
        df_size = df.memory_usage(deep=True).sum()

        # Determine the partition size based on dataset size thresholds
        partition_enum = DatasetSizeThreshold.get_partition_size(df_size=df_size)

        # Get the partition size
        partition_size = partition_enum.partition_size * adjust

        # Get the number of CPU cores (logical cores for maximum parallelism)
        num_cores = psutil.cpu_count(logical=True)

        # Calculate the number of partitions
        npartitions = min(
            max(
                num_cores,
                math.ceil(df_size / partition_size),
            ),
            10000,
        )

        # Return results in MB for easier interpretation
        partition_size_mb = partition_size / (1024**2)
        return npartitions, partition_size_mb
