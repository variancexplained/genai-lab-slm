#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/core/data_structure.py                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 08:16:41 pm                                              #
# Modified   : Thursday November 21st 2024 12:44:21 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from enum import Enum
from typing import TypeAlias, Union

import pandas as pd
from pyspark.sql import DataFrame

# ------------------------------------------------------------------------------------------------ #
DataFrameType: TypeAlias = Union[pd.DataFrame, DataFrame]


# ------------------------------------------------------------------------------------------------ #
class DataStructure(Enum):
    PANDAS = "pandas"
    SPARK = "spark"
