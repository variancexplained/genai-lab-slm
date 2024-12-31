#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/asset/dataset/__init__.py                                                 #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday September 9th 2024 04:21:25 pm                                               #
# Modified   : Tuesday December 31st 2024 07:00:28 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Init Module for Dataset Shared Classes"""
from __future__ import annotations

from enum import Enum

# ------------------------------------------------------------------------------------------------ #
#                                     FILE FORMATS                                                 #
# ------------------------------------------------------------------------------------------------ #


class FileFormat(Enum):
    CSV = "csv"
    PARQUET = "parquet"

    @classmethod
    def from_value(cls, value) -> FileFormat:
        """Finds the enum member based on a given value"""
        for member in cls:
            if member.value == value:
                return member
        raise ValueError(f"No matching {cls.__name__} for value: {value}")


# ------------------------------------------------------------------------------------------------ #
#                                   DATAFRAME TYPE                                                 #
# ------------------------------------------------------------------------------------------------ #
class DFType(Enum):

    PANDAS = "pandas"
    SPARK = "spark"

    @classmethod
    def from_value(cls, value) -> FileFormat:
        """Finds the enum member based on a given value"""
        for member in cls:
            if member.value == value:
                return member
        raise ValueError(f"No matching {cls.__name__} for value: {value}")
