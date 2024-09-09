#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appvocai/core/artifact.py                                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday June 30th 2024 10:32:43 pm                                                   #
# Modified   : Monday September 9th 2024 09:41:24 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Entity Module"""
from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass

from appvocai.utils.data import DataClass


# ------------------------------------------------------------------------------------------------ #
@dataclass
class Artifact(DataClass):
    """Base class for all entities"""

    oid: str = None
    name: str = None
    description: str = None
    phase: str = None
    stage: str = None
    creator: str = None
    created: str = None

    @abstractmethod
    def validate(self) -> None:
        """Validates the entity. Should be called when exporting data to be persisted"""
