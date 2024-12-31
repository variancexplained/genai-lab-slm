#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/core/dtypes.py                                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday August 26th 2024 10:17:42 pm                                                 #
# Modified   : Tuesday December 31st 2024 11:38:04 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data Types Module """
from typing import Tuple

import numpy as np
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ------------------------------------------------------------------------------------------------ #
#                                           DATA TYPES                                             #
# ------------------------------------------------------------------------------------------------ #
IMMUTABLE_TYPES: Tuple = (
    str,
    int,
    float,
    bool,
    type(None),
    np.int16,
    np.int32,
    np.int64,
    np.int8,
    np.float16,
    np.float32,
    np.float64,
    np.float128,
)
SEQUENCE_TYPES: Tuple = (
    list,
    tuple,
)
# ------------------------------------------------------------------------------------------------ #
NUMERICS = [
    "int16",
    "int32",
    "int64",
    "float16",
    "float32",
    "float64",
    np.int16,
    np.int32,
    np.int64,
    np.int8,
    np.float16,
    np.float32,
    np.float64,
    np.float128,
]

# ------------------------------------------------------------------------------------------------ #
DTYPES = {
    "id": "string",
    "app_id": "string",
    "app_name": "string",
    "category_id": "category",
    "category": "category",
    "author": "string",
    "rating": "int16",
    "content": "string",
    "vote_count": "int64",
    "vote_sum": "int64",
    "date": "datetime64[ns]",
}

# ------------------------------------------------------------------------------------------------ #
SPARK_SCHEMA_DEFAULT = StructType(
    [
        StructField("id", StringType(), False),
        StructField("app_id", StringType(), False),
        StructField("app_name", StringType(), False),
        StructField("category_id", StringType(), False),
        StructField("author", StringType(), False),
        StructField("rating", DoubleType(), False),
        StructField("content", StringType(), False),
        StructField("vote_sum", LongType(), False),
        StructField("vote_count", LongType(), False),
        StructField("date", TimestampType(), False),
        StructField("category", StringType(), False),
    ]
)
