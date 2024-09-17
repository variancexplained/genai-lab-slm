#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/domain/value_objects/file.py                                              #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 15th 2024 01:49:56 am                                              #
# Modified   : Monday September 16th 2024 09:07:51 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""File Value Object Module"""
from enum import Enum


# ------------------------------------------------------------------------------------------------ #
class FileFormat(Enum):
    CSV = ".csv"
    TSV = ".tsv"
    PICKLE = ".pickle"
    PARQUET = ".parquet"
    PARQUET_PARTITIONED = ""


# ------------------------------------------------------------------------------------------------ #
class KVSType(Enum):
    CACHE = "cache"
