#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/domain/task/base.py                                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday September 9th 2024 04:21:45 pm                                               #
# Modified   : Monday September 9th 2024 04:23:54 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Abstract Base Classes for the Application Layer"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


# ------------------------------------------------------------------------------------------------ #
#                                           TASK                                                   #
# ------------------------------------------------------------------------------------------------ #
class Task(ABC):

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @abstractmethod
    def run(self, *args: Any, **kwargs: Any) -> Any:
        """Execute the task.

        This method defines the core functionality of the task. Subclasses
        must implement this method to define the specific behavior of the task.

        Parameters:
            args (Any): Positional arguments
            kwargs (Any): Keyword arguments

        Returns:
            Any: The result of executing the task.
        """
