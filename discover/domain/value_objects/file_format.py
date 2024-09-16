#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/storage/local/file_format.py                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 15th 2024 01:49:56 am                                              #
# Modified   : Sunday September 15th 2024 01:52:21 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""File Format Module"""
from enum import Enum


# ------------------------------------------------------------------------------------------------ #
class FileFormat(Enum):
    CSV = ".csv"
    TSV = ".tsv"
    PICKLE = ".pickle"
    PARQUET = ".parquet"
    PARQUET_PARTITIONED = ""
