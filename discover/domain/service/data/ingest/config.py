#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/domain/service/data/ingest/config.py                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday September 14th 2024 02:14:39 am                                            #
# Modified   : Saturday September 14th 2024 04:28:06 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from dataclasses import dataclass, field

from discover.domain.value_objects.config import DataConfig, ServiceConfig
from discover.domain.value_objects.lifecycle import Stage

# ------------------------------------------------------------------------------------------------ #


@dataclass
class IngestConfig(ServiceConfig):
    """Data processing configuration for the Stage"""

    stage: Stage = Stage.INGEST
    source_data_config: DataConfig
    target_data_config: DataConfig
    partition_cols: list = field(default_factory=lambda: ["category"])
    text_column: str = "content"
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
