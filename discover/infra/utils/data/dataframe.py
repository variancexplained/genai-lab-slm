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
# Modified   : Friday October 18th 2024 02:44:28 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""DataFrame Utility Module"""

from typing import Union

import pandas as pd
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
