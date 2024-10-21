#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/base/stage.py                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday September 20th 2024 08:14:05 pm                                              #
# Modified   : Monday October 21st 2024 12:06:22 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Stage Base Class  Module"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List

from discover.core.flow import PhaseDef, StageDef
from discover.core.namespace import NestedNamespace
from discover.flow.base.task import Task, TaskBuilder


# ------------------------------------------------------------------------------------------------ #
#                                        STAGE                                                     #
# ------------------------------------------------------------------------------------------------ #
class Stage(ABC):
    """Abstract base class for Stage pipelines."""

    def __init__(
        self,
        source_config: dict,
        destination_config: dict,
        tasks: List[Task],
        force: bool = False,
    ) -> None:
        self._source_config = NestedNamespace(source_config)
        self._destination_config = NestedNamespace(destination_config)
        self._tasks = tasks
        self._force = force

    @property
    @abstractmethod
    def phase(self) -> PhaseDef:
        """Phase"""

    @property
    @abstractmethod
    def stage(self) -> StageDef:
        """Stage"""

    @abstractmethod
    def run(self) -> str:
        """Stage execution"""

    @classmethod
    def build(cls, stage_config: dict, force: bool = False) -> Stage:
        tasks = [
            TaskBuilder.build(task_config) for task_config in stage_config["tasks"]
        ]
        return cls(
            source_config=stage_config["source_config"],
            destination_config=stage_config["destination_config"],
            tasks=tasks,
            force=force,
        )
