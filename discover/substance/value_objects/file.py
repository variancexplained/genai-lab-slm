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
# Modified   : Thursday September 19th 2024 01:30:35 pm                                            #
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


# ------------------------------------------------------------------------------------------------ #
class ExistingDataBehavior(Enum):
    ERROR = "error"
    OVERWRITE_OR_IGNORE = "overwrite_or_ignore"
    DELETE_MATCHING = "delete_matching"
