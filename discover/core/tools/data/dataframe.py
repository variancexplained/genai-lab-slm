#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/core/tools/data/dataframe.py                                              #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday September 20th 2024 04:35:45 pm                                              #
# Modified   : Friday September 20th 2024 07:17:11 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""DataFrame Utility Module"""


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
