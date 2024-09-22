#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/dynamics/phase/base.py                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday September 21st 2024 04:48:12 pm                                            #
# Modified   : Saturday September 21st 2024 10:00:08 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from __future__ import annotations

from abc import ABC, abstractmethod

from discover.dynamics.stage.base import Stage


# ------------------------------------------------------------------------------------------------ #
class Phase(ABC):
    """Abstract base class for phases"""

    def __init__(self) -> None:

        self._stages = []

    def add_stage(self, stage: Stage) -> None:
        self._stages.append(stage)

    @abstractmethod
    def run(self) -> None:
        """Executes the stages in the phase."""


# ------------------------------------------------------------------------------------------------ #
class PhaseBuilder(ABC):
    def __init__(self) -> None:
        self._phase: Phase = None
        self._stage = None

    @property
    def phase(self) -> Phase:
        return self._phase

    @abstractmethod
    def add_stage(self, stage: Stage) -> None:
        self._stage = stage
