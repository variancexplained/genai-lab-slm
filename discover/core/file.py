#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/core/file.py                                                              #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday January 2nd 2025 06:41:52 am                                               #
# Modified   : Thursday January 2nd 2025 06:42:38 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""Core File Module"""
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
