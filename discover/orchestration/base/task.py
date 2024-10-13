#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/orchestration/base/task.py                                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday September 10th 2024 04:49:44 pm                                             #
# Modified   : Saturday October 12th 2024 08:46:30 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Abstract Base Classes for Task classes """
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from discover.core.flow import PhaseDef, StageDef
from discover.core.namespace import NestedNamespace


# ------------------------------------------------------------------------------------------------ #
#                                           TASK                                                   #
# ------------------------------------------------------------------------------------------------ #
class Task(ABC):

    def __init__(
        self, phase: str, stage: str, task_config: Optional[Dict] = None
    ) -> None:
        self._phase = PhaseDef.from_value(phase)
        self._stage = StageDef.from_value(stage)
        self._task_config = NestedNamespace(task_config)
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @property
    def phase(self) -> PhaseDef:
        return self._phase

    @property
    def stage(self) -> StageDef:
        return self._stage

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @property
    def logger(self) -> logging.Logger:
        return self._logger

    @property
    def config(self) -> NestedNamespace:
        return self._task_config

    @abstractmethod
    def run(self, *args, data: Any, **kwargs) -> Any:
        """"""
