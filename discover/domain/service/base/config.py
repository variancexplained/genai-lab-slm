#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/domain/service/base/config.py                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday September 10th 2024 04:49:44 pm                                             #
# Modified   : Tuesday September 10th 2024 04:50:59 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Abstract Base Class for Data Processing Stage Configurations"""
from abc import ABC
from dataclasses import dataclass

# ------------------------------------------------------------------------------------------------ #
#                                       STAGE CONFIG                                               #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class StageConfig(ABC):
    """Abstract base class for data preprocessing stage configurations."""

    name: str = None
    source_directory: str = None
    source_filename: str = None
    target_directory: str = None
    target_filename: str = None
    force: bool = False
