#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/dynamics/data_prep/norm/config.py                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday October 10th 2024 12:12:19 pm                                              #
# Modified   : Thursday October 10th 2024 03:33:17 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #

from dataclasses import field
from typing import dataclass_transform

from discover.core.data_class import DataClass
from discover.core.flow import DataPrepStageDef, PhaseDef


# ------------------------------------------------------------------------------------------------ #
@dataclass_transform
class DataNormConfig(DataClass):
    """Configuration for the Data Normalization Stage of Data Prep Phase"""

    source_dataset: str = f"{PhaseDef.DATAPREP.value}_{DataPrepStageDef.RAW.value}"
    source_filename: str = None
    target_directory: str = "01_norm/reviews"
    target_filename: str = None
    partition_cols: str = "category"
    text_column: str = "content"
    force: bool = False
    encoding_sample: float = 0.01
    random_state: int = 22
    datatypes: dict = field(
        default_factory=lambda: {
            "id": "string",
            "app_id": "string",
            "app_name": "string",
            "category_id": "category",
            "category": "category",
            "author": "string",
            "rating": "int16",
            "content": "string",
            "vote_count": "int64",
            "vote_sum": "int64",
            "date": "datetime64[ms]",
        }
    )
