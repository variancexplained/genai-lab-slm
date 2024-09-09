#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appvocai/core/workflow/pipeline.py                                                 #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday July 5th 2024 04:57:31 pm                                                    #
# Modified   : Tuesday August 27th 2024 10:54:14 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from abc import ABC, abstractmethod
from dataclasses import dataclass

from appvocai import DataClass
from appvocai.core.workflow.task import Task


# ------------------------------------------------------------------------------------------------ #
#                                    PIPELINE CONFIG                                               #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class PipelineConfig(DataClass):
    source: str  # Filepath to the source file, relative to the environment data root.
    destination: (
        str  # Filepath to the destination file, relative to the environment data root.
    )
    force: bool = False


# ------------------------------------------------------------------------------------------------ #
#                                        PIPELINE                                                  #
# ------------------------------------------------------------------------------------------------ #
class Pipeline(ABC):
    """Abstract base class for Pipeline classes."""

    @abstractmethod
    def add_task(self, task: Task):
        """Add a task to the pipeline.

        Args:
            task (Task): A task object to add to the pipeline.
        """

    @abstractmethod
    def run(self):
        """Execute the pipeline tasks in sequence."""

    @abstractmethod
    def start_pipeline(self) -> None:
        """Logic to execute at the start of the pipeline."""

    @abstractmethod
    def stop_pipeline(self) -> None:
        """Logic to execute at the stop of the pipeline."""

    @abstractmethod
    def endpoint_exists(self) -> bool:
        """Returns True if the endpoint already exists."""
