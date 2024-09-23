#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/dal/base.py                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 05:39:55 pm                                              #
# Modified   : Sunday September 22nd 2024 08:18:42 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from abc import ABC, abstractmethod
from typing import Any

from discover.core.flow import PhaseDef, StageDef
from discover.element.base.define import Element


class DAO(ABC):

    @abstractmethod
    def create(self, element: Element, **kwargs) -> None:
        """Adds an element to the underlying storage mechanism."""

    @abstractmethod
    def read(self, phase: PhaseDef, stage: StageDef, name: str, **kwargs) -> Any:
        """Reads an element from"""
