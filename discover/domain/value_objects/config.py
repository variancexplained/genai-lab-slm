#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/application/service/base/config.py                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday September 10th 2024 04:49:55 pm                                             #
# Modified   : Friday September 13th 2024 09:36:43 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Abstract Base Class for Data Processing Stage Configurations"""

from dataclasses import dataclass

from discover.core.data import DataClass
from discover.domain.service.base.repo import Repo
from discover.domain.value_objects.lifecycle import Stage

# ------------------------------------------------------------------------------------------------ #


@dataclass
class DataConfig(DataClass):
    repo: Repo
    stage: Stage
    name: str


# ------------------------------------------------------------------------------------------------ #
@dataclass
class ServiceConfig(DataClass):
    """Abstract base class for data preprocessing stage configurations."""

    source_data_config: DataConfig
    target_data_config: DataConfig
    force: bool = False
