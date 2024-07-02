#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/infrastructure/profiling/profiler.py                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday May 26th 2024 01:45:37 am                                                    #
# Modified   : Monday July 1st 2024 12:29:35 am                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Profiler Module"""
import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Optional

import pandas as pd
import psutil

from appinsight.infrastructure.persist.file.io import IOService
from appinsight.infrastructure.profiling.profile import TaskProfile

# ------------------------------------------------------------------------------------------------ #


class BaseProfiler(ABC):
    @property
    @abstractmethod
    def profile(self) -> Any:
        """Returns the results of the profiling session."""
        pass

    @abstractmethod
    def start(self) -> None:
        """Starts the profiling session."""
        pass

    @abstractmethod
    def stop(self, result: Any = None) -> None:
        """Ends the profiling session."""
        pass


# ------------------------------------------------------------------------------------------------ #
class TaskProfiler(BaseProfiler):
    """Profiles a task..

    This profiler is suited for tasks that are computationally intense.
    Metrics reported include CPU and memory usage, disk IO, and runtime.

    Args:
        task (str): Task name being profiled.
        args (tuple): Positional arguments passed to the task.
        kwargs (dict): Keyword arguments passed to the task.
        io (IOService, optional): IOService instance for reading configuration. Defaults to IOService.
    """

    __TASK_CONFIG_FILEPATH = "config/task_config.csv"

    def __init__(self, task: str, args, kwargs, io: IOService = IOService) -> None:
        """Initializes the TaskProfiler.

        Args:
            task (str): Task name being profiled.
            args (tuple): Positional arguments passed to the task.
            kwargs (dict): Keyword arguments passed to the task.
            io (IOService, optional): IOService instance for reading configuration. Defaults to IOService.
        """
        super().__init__()
        self._task = task
        self._args = args
        self._kwargs = kwargs
        self._io = io

        self._profile = None
        self._task_config = self._io.read(filepath=self.__TASK_CONFIG_FILEPATH)

        self._start_time = None
        self._cpu_start_time = None
        self._memory_start_info = None
        self._disk_start_io = None

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @property
    def profile(self) -> Any:
        """Returns the results of the profiling session.

        Returns:
            Any: The profiling results.
        """
        return self._profile

    def start(self) -> None:
        """Starts the profiling session."""
        try:
            self._process = psutil.Process(os.getpid())
            self._start_time = datetime.now()
            self._cpu_start_time = self._process.cpu_times()
            self._memory_start_info = self._process.memory_full_info()
            self._disk_start_io = self._process.io_counters()
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess) as e:
            self._logger.exception(f"Error starting profiling session: {e}")
            # Handle or log the error as needed
            raise

    def stop(self, result: Any = None) -> None:
        """Ends the profiling session and computes metrics.

        Args:
            result (Any, optional): The result produced by the task, used to calculate output records. Defaults to None.
        """
        try:
            timestamp = datetime.timestamp(datetime.now())
            end_time = datetime.now()

            cpu_end_time = self._process.cpu_times()
            memory_end_info = self._process.memory_full_info()
            disk_end_io = self._process.io_counters()

            cpu_time_used = (cpu_end_time.user - self._cpu_start_time.user) + (
                cpu_end_time.system - self._cpu_start_time.system
            )
            runtime = (end_time - self._start_time).total_seconds()

            cpu_usage_pct = (cpu_time_used / runtime) * 100 if runtime > 0 else 0
            memory_usage = memory_end_info.uss - self._memory_start_info.uss
            disk_read_bytes = disk_end_io.read_bytes - self._disk_start_io.read_bytes
            disk_write_bytes = disk_end_io.write_bytes - self._disk_start_io.write_bytes
            disk_total_bytes = disk_write_bytes + disk_read_bytes

            input_records = 0
            output_records = 0
            input_records_per_second = 0
            output_records_per_second = 0

            dataframe = self._find_input_data()
            if dataframe is not None:
                input_records = len(dataframe)
                input_records_per_second = input_records / runtime if runtime > 0 else 0

            if isinstance(result, pd.DataFrame):
                output_records = len(result)
                output_records_per_second = (
                    output_records / runtime if runtime > 0 else 0
                )

            task_info = self._get_task_info()

            self._profile = TaskProfile(
                phase=task_info["phase"],
                stage=task_info["stage"],
                task=self._task,
                runtime=runtime,
                cpu_usage_pct=cpu_usage_pct,
                memory_usage=memory_usage,
                disk_read_bytes=disk_read_bytes,
                disk_write_bytes=disk_write_bytes,
                disk_total_bytes=disk_total_bytes,
                input_records=input_records,
                output_records=output_records,
                input_records_per_second=input_records_per_second,
                output_records_per_second=output_records_per_second,
                timestamp=timestamp,
            )
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess) as e:
            self._logger.exception(f"Error stopping profiling session: {e}")
            # Handle or log the error as needed
            raise

    def _get_task_info(self) -> pd.Series:
        """Returns the phase and stage for a given task.

        Returns:
            pd.Series: Series containing phase and stage information.

        Raises:
            ValueError: If no task info is found for the given task.
        """
        task_info = self._task_config.loc[self._task_config["task"] == self._task]
        if task_info.empty:
            raise ValueError(
                f"No task info found for {self._task}. Add the task info to task_config.csv"
            )
        return task_info.iloc[0]

    def _find_input_data(self) -> Optional[pd.DataFrame]:
        """Finds the input DataFrame from the provided arguments.

        Returns:
            Optional[pd.DataFrame]: The input DataFrame if found, else None.
        """
        dataframe = next(
            (arg for arg in self._args if isinstance(arg, pd.DataFrame)), None
        )
        if dataframe is None:
            dataframe = next(
                (
                    value
                    for key, value in self._kwargs.items()
                    if isinstance(value, pd.DataFrame)
                ),
                None,
            )
        return dataframe
