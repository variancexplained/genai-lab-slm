#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/workflow/task.py                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday May 23rd 2024 02:05:34 pm                                                  #
# Modified   : Friday June 28th 2024 04:15:24 pm                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Task Base Class Module"""
import logging
from abc import ABC, abstractmethod
from datetime import datetime
import shelve
from typing import Any, Dict

from appinsight.utils.datetime import convert_seconds_to_hms
from appinsight.utils.print import Printer
from appinsight.utils.env import EnvManager


# ------------------------------------------------------------------------------------------------ #
#                                           TASK                                                   #
# ------------------------------------------------------------------------------------------------ #
class Task(ABC):

    def __init__(self, *args, **kwargs) -> None:
        self._start = None
        self._stop = None
        self._runtime = None
        self._metrics = None
        self._printer = Printer()
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @property
    def metrics(self) -> Dict:
        return self._metrics

    @property
    def logger(self) -> logging.Logger:
        return self._logger

    def start_task(self) -> None:
        self._start = datetime.now()

    def stop_task(self) -> None:
        self._stop = datetime.now()
        self._runtime = convert_seconds_to_hms(
            (self._stop - self._start).total_seconds()
        )
        self._metrics = {
            "task": self.name,
            "start": self._start,
            "stop": self._stop,
            "runtime": self._runtime,
        }
        print(f"Task {self.name} completed successfully. Runtime: {self._runtime}")

    def execute(self, *args: Any, **kwargs: Any) -> Any:
        """Wraps the execute tasks with metrics capture and calculations"""
        self.start_task()
        data = self.execute_task(*args, **kwargs)
        self.stop_task()
        return data

    @abstractmethod
    def execute_task(self, *args: Any, **kwargs: Any) -> Any:
        """Execute the task.

        This method defines the core functionality of the task. Subclasses
        must implement this method to define the specific behavior of the task.

        Parameters:
            args (Any): Positional arguments
            kwargs (Any): Keyword arguments

        Returns:
            Any: The result of executing the task.
        """

    def reset_cache(self):
        # Get the current environment
        env = EnvManager().get_environment()
        # Set the shelve file
        shelf_file = f"cache/{env}/cache"
        # Check if the shelve file exists
        try:
            # Open the shelve file
            with shelve.open(shelf_file) as cache:
                # Get the class name
                class_name = type(self).__name__
                # Iterate over cache keys
                keys_to_remove = [key for key in cache.keys() if class_name in key]
                # Remove keys from the cache
                for key in keys_to_remove:
                    del cache[key]
        except Exception as e:
            msg = f"Exception occurred in reset_cache.\n{e}"
            self.logger.exception(msg)
            raise RuntimeError(msg)
