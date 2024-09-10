#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/monitor/profile.py                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday September 9th 2024 07:42:03 pm                                               #
# Modified   : Monday September 9th 2024 10:36:46 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""TaskProfile Module"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from discover.core.data import DataClass


# ------------------------------------------------------------------------------------------------ #
@dataclass
class TaskProfile(DataClass):

    env: str  # Environment (e.g., production, staging, development)
    stage: str  # Stage in the pipeline (e.g., data cleaning, analysis)
    task_name: str  # Name of the task/class being profiled (e.g., AnonymizeReviewsTask)
    task_start_time: datetime  # Task start time
    task_end_time: datetime  # Task end time
    runtime_seconds: float  # Total runtime of the task (in seconds)
    cpu_cores: int  # Total number of cpu cores in the machine.
    cpu_user_utilization: float  # Time spent in user space (in seconds)
    cpu_system_utilization: float  # Time spent in system space (in seconds)
    memory_usage_peak_mb: float  # Peak memory usage (in MB)
    memory_allocations: int  # Number of memory allocations during the task
    file_read_bytes: int  # Total bytes read from files during the task
    file_write_bytes: int  # Total bytes written to files during the task
    io_wait_time_seconds: (
        float  # Time spent waiting for I/O operations to complete (in seconds)
    )
    network_data_sent_bytes: int  # Total data sent over the network (in bytes)
    network_data_received_bytes: int  # Total data received over the network (in bytes)
    exceptions_raised: int = (
        0  # Number of exceptions raised during task execution (default 0)
    )
    retry_count: int = 0  # Number of retries or re-executions of the task (default 0)
    id: Optional[int] = None  # Unique identifier for each profiling record
