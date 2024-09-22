#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/element/base/define.py                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday September 21st 2024 10:21:05 pm                                            #
# Modified   : Sunday September 22nd 2024 12:26:55 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Base Module for the Element Dimension"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from discover.core.invariants.flow import PhaseDef, StageDef
from discover.core.structure.data_class import DataClass
from discover.infra.tools.date_time.format import ThirdDateFormatter

# ------------------------------------------------------------------------------------------------ #
dt4mtr = ThirdDateFormatter()


# ------------------------------------------------------------------------------------------------ #
@dataclass
class Element(DataClass):
    """
    A base class representing a generic element in the system.

    Attributes:
    - id (int): Unique sequential numeric identifier for the element.
    - name (str): The name of the element.
    - phase (PhaseDef): The phase in which the element is created.
    - stage (StageDef): The stage in which the element resides.
    - created (datetime): The timestamp when the element was created.
    - persisted (datetime): The timestamp when the element was persisted.
    - accessed (datetime): The timestamp when the element was last accessed.
    - build_duration (float): Time taken to build the element, defaults to 0.
    - content (Any): The actual content or data associated with the element.
    """

    id: int
    name: str
    phase: PhaseDef
    stage: StageDef
    content: Any
    created: datetime
    persisted: Optional[datetime] = None
    accessed: Optional[datetime] = None
    build_duration: float = 0

    def __post_init__(self) -> None:
        self.created = datetime.now()

    def description(self) -> str:
        return f"{self.phase.description} - {self.stage.description} {self.name} {self.__class__.__name__} created on {dt4mtr.to_HTTP_format(datetime.now())}."

    def fullname(self) -> str:
        return f"{self.phase.value}-{self.stage.value}_{self.__class__.__name__.lower()}_{self.name}_{self.created.strftime('%Y%m%d')-{str(self.id).zfill(4)}}"
