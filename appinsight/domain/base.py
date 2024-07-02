#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/domain/base.py                                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday June 30th 2024 10:32:43 pm                                                   #
# Modified   : Tuesday July 2nd 2024 11:58:58 am                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Entity Module"""
from __future__ import annotations

from abc import abstractmethod

from appinsight.utils.data import DataClass


# ------------------------------------------------------------------------------------------------ #
class Entity(DataClass):
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
