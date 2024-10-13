#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/dynamics/observability/profiler.py                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday September 10th 2024 12:36:42 am                                             #
# Modified   : Sunday September 22nd 2024 04:25:19 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from datetime import datetime
from functools import wraps
from typing import Callable

import psutil  # For gathering system resource usage

from discover.application.ops.utils import find_task
from discover.container import DiscoverContainer
from discover.dynamics.observability.profile import Profile

# ------------------------------------------------------------------------------------------------ #
# Initialize the repository once at the module level
container = DiscoverContainer()
profile_repo = container.repo.profile()
# ------------------------------------------------------------------------------------------------ #


def profiler(func: Callable) -> Callable:
    """
    A decorator to profile Task objects by measuring performance metrics.

    This decorator collects metrics such as CPU utilization, memory usage, I/O stats,
    and network activity during the execution of a decorated task's `run` method.
    After the task finishes, it stores the collected metrics in the profile repository.

    Metrics Collected:
        - Task runtime (start time, end time, total duration)
        - CPU utilization (user and system)
        - Memory usage (peak)
        - File I/O (read/write bytes)
        - Network data (sent/received bytes)
        - Exception count
        - Retry count (if applicable)

    Args:
        func (Callable): The `run` method of the task object to be decorated.

    Returns:
        Callable: A wrapped version of the `run` method with profiling logic.
    """

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        """
        Wrapper function that executes the task's `run` method, measures system resource
        usage during its execution, and stores profiling data in the profile repository.

        Args:
            self: The instance of the class containing the `run` method.
            *args: Positional arguments passed to the `run` method.
            **kwargs: Keyword arguments passed to the `run` method.

        Returns:
            The result of the original `run` method.
        """

        # Capture start time and system metrics before task execution
        start_time = datetime.now()
        cpu_cores = psutil.cpu_count(logical=True)  # Get the total number of CPU cores
        memory_before = psutil.virtual_memory().used / (1024**2)  # Memory in MB
        io_before = psutil.disk_io_counters()  # Initial disk I/O stats
        net_before = psutil.net_io_counters()  # Initial network I/O stats

        exceptions_raised = 0  # Track the number of exceptions raised during the task

        try:
            # Execute the original run method
            result = func(self, *args, **kwargs)
        except Exception:
            exceptions_raised += 1
            raise
        finally:
            # Capture end time and system metrics after task execution
            end_time = datetime.now()
            runtime_seconds = (end_time - start_time).total_seconds()
            memory_after = psutil.virtual_memory().used / (1024**2)
            io_after = psutil.disk_io_counters()
            net_after = psutil.net_io_counters()

            # Compute system utilization during the task
            cpu_user_time = psutil.cpu_times().user  # Time spent in user mode
            cpu_system_time = psutil.cpu_times().system  # Time spent in kernel mode
            memory_peak_mb = max(memory_before, memory_after)  # Peak memory usage
            file_read_bytes = io_after.read_bytes - io_before.read_bytes
            file_write_bytes = io_after.write_bytes - io_before.write_bytes
            io_wait_time_seconds = io_after.write_time - io_before.write_time
            network_data_sent_bytes = net_after.bytes_sent - net_before.bytes_sent
            network_data_received_bytes = net_after.bytes_recv - net_before.bytes_recv
            cpu_user_utilization = cpu_user_time / (runtime_seconds * cpu_cores)
            cpu_system_utilization = cpu_system_time / (runtime_seconds * cpu_cores)

            # Find the Task object and return the context
            task = find_task(args, kwargs)

            # Create the Profile object with the computed metrics
            profile = Profile(
                phase=task.context.phase.description,
                stage=task.context.stage.description,
                task=task.context.task.__name__,
                start_time=start_time,
                end_time=end_time,
                runtime_seconds=runtime_seconds,
                cpu_cores=cpu_cores,
                cpu_user_utilization=cpu_user_utilization,
                cpu_system_utilization=cpu_system_utilization,
                memory_usage_peak_mb=memory_peak_mb,
                memory_allocations=0,  # Placeholder, as memory allocations are not tracked here
                file_read_bytes=file_read_bytes,
                file_write_bytes=file_write_bytes,
                io_wait_time_seconds=io_wait_time_seconds,
                network_data_sent_bytes=network_data_sent_bytes,
                network_data_received_bytes=network_data_received_bytes,
                exceptions_raised=exceptions_raised,
            )

            # Persist the profile data to the profile repository
            profile_repo.add(profile)

        return result  # Return the result of the original `run` method

    return wrapper
