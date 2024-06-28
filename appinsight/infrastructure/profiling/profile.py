#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/infrastructure/profiling/profile.py                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday May 26th 2024 01:45:37 am                                                    #
# Modified   : Friday May 31st 2024 02:53:51 am                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #

from dataclasses import dataclass

from appinsight.utils.data import DataClass


# ------------------------------------------------------------------------------------------------ #
@dataclass
class Profile(DataClass):
    """Base class for Profile classes."""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class TaskProfile(Profile):
    """Encapsulates a profile for tasks."""

    task: str
    stage: str
    phase: str
    runtime: float
    cpu_usage_pct: float
    memory_usage: float
    disk_read_bytes: int
    disk_write_bytes: int
    disk_total_bytes: int
    input_records: int
    output_records: int
    input_records_per_second: float
    output_records_per_second: float
    timestamp: int


# ------------------------------------------------------------------------------------------------ #
@dataclass
class TaskProfileDTO(DataClass):
    """Encapsulates a profile DTO tasks. The DTO is returned from the data access layer read."""

    id: int
    task: str
    stage: str
    phase: str
    runtime: float
    cpu_usage_pct: float
    memory_usage: float
    disk_read_bytes: int
    disk_write_bytes: int
    disk_total_bytes: int
    input_records: int
    output_records: int
    input_records_per_second: float
    output_records_per_second: float
    timestamp: int
