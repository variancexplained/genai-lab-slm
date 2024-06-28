#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/workflow/config.py                                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday May 31st 2024 02:31:23 am                                                    #
# Modified   : Sunday June 2nd 2024 09:45:32 am                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from abc import ABC
from dataclasses import dataclass

import pandas as pd

# ------------------------------------------------------------------------------------------------ #
#                                       STAGE CONFIG                                               #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class StageConfig(ABC):
    """Data processing stage configuration"""

    name: str = None
    source_directory: str = None
    source_filename: str = None
    target_directory: str = None
    target_filename: str = None
    force: bool = False
