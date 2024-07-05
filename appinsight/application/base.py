#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/application/base.py                                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday June 1st 2024 07:16:33 pm                                                  #
# Modified   : Thursday July 4th 2024 07:50:29 pm                                                  #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Abstract Base Classes for the Application Layer"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict

from appinsight.shared.utils.datetime import convert_seconds_to_hms

# ------------------------------------------------------------------------------------------------ #
#                                       STAGE CONFIG                                               #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class StageConfig(ABC):
    """Abstract base class for data preprocessing stage configurations."""

    name: str = None
    source_directory: str = None
    source_filename: str = None
    target_directory: str = None
    target_filename: str = None
    force: bool = False


# ------------------------------------------------------------------------------------------------ #
#                                           TASK                                                   #
# ------------------------------------------------------------------------------------------------ #
class Task(ABC):

    def __init__(self, *args, **kwargs) -> None:
        self._start = None
        self._stop = None
        self._runtime = None
        self._metrics = None
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

    def run(self, *args: Any, **kwargs: Any) -> Any:
        """Wraps the execute tasks with metrics capture and calculations"""
        self.start_task()
        data = self.run_task(*args, **kwargs)
        self.stop_task()
        return data

    @abstractmethod
    def run_task(self, *args: Any, **kwargs: Any) -> Any:
        """Execute the task.

        This method defines the core functionality of the task. Subclasses
        must implement this method to define the specific behavior of the task.

        Parameters:
            args (Any): Positional arguments
            kwargs (Any): Keyword arguments

        Returns:
            Any: The result of executing the task.
        """
