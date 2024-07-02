#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/domain/config.py                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday July 1st 2024 01:09:56 am                                                    #
# Modified   : Monday July 1st 2024 04:55:03 am                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Domain Config Module"""
import logging
from dataclasses import dataclass

from appinsight.domain.enums import Phase, Stage
from appinsight.utils.data import DataClass


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatasetConfig(DataClass):
    stage: str
    creator: str
    name: str = "reviews"
    description: str = None
    phase: str = "PREP"

    def __post_init__(self) -> None:
        # Validate phase
        if self.phase not in Phase.__members__.keys():
            msg = f"Value {self.phase} for phase is invalid. Valid values  are {Phase.__members__.keys()}"
            logging.exception(msg)
            raise ValueError(msg)
        # Validate stage
        if self.stage not in Stage.__members__.keys():
            msg = f"Value {self.stage} for stage is invalid. Valid values  are {Stage.__members__.keys()}"
            logging.exception(msg)
            raise ValueError(msg)
