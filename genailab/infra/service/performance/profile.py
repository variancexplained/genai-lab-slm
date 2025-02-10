#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab                                                                           #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/infra/service/performance/profile.py                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday September 9th 2024 07:42:03 pm                                               #
# Modified   : Saturday February 8th 2025 10:43:30 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Profile Module"""
from __future__ import annotations

from datetime import datetime
from typing import Optional, Union

from pydantic.dataclasses import dataclass

from genailab.core.dstruct import DataClass
from genailab.core.flow import (
    EDAStageDef,
    ModelingStageDef,
    OpportunityStageDef,
    Phase,
    SentimentStageDef,
    StageDef,
)


# ------------------------------------------------------------------------------------------------ #
@dataclass(config=dict(arbitrary_types_allowed=True))
class Profile(DataClass):
    phase: Phase  # Phase i.e. DataPrep, Analysis
    stage: Union[
        StageDef,
        Phase,
        EDAStageDef,
        ModelingStageDef,
        SentimentStageDef,
        OpportunityStageDef,
    ]  # Stage within Phase, i.e. DQA
    task: str  # Class name for the task to which the context applies.
    start_time: datetime  # Task start time
    end_time: datetime  # Task end time
    runtime_seconds: float  # Total runtime of the process (in seconds)
    cpu_cores: int  # Total number of cpu cores in the machine.
    cpu_user_utilization: float  # Time spent in user space (in seconds)
    cpu_system_utilization: float  # Time spent in system space (in seconds)
    memory_usage_peak_mb: float  # Peak memory usage (in MB)
    memory_allocations: int  # Number of memory allocations during the process
    file_read_bytes: int  # Total bytes read from files during the process
    file_write_bytes: int  # Total bytes written to files during the process
    io_wait_time_seconds: (
        float  # Time spent waiting for I/O operations to complete (in seconds)
    )
    network_data_sent_bytes: int  # Total data sent over the network (in bytes)
    network_data_received_bytes: int  # Total data received over the network (in bytes)
    exceptions_raised: int = (
        0  # Number of exceptions raised during process execution (default 0)
    )
    id: Optional[int] = None  # Unique identifier for each run.
