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
# Modified   : Monday December 30th 2024 03:00:13 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data Types Module """
from typing import Tuple

import numpy as np

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
