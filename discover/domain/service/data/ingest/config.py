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
# Modified   : Saturday September 14th 2024 05:37:07 am                                            #
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
    """
    Configuration class for the data ingestion process.

    This class defines the configuration parameters required for the data ingestion stage
    in the pipeline. It includes source and target data configurations, partitioning columns,
    text column details, encoding sample rate, random seed, and data type mappings for the input data.

    Attributes:
    -----------
    stage : Stage
        The stage of the processing pipeline, which is set to INGEST.
    source_data_config : DataConfig
        Configuration object for the source data to be ingested.
    target_data_config : DataConfig
        Configuration object for the target data after ingestion.
    partition_cols : list
        A list of column names used for partitioning the data. Defaults to ["category"].
    text_column : str
        The name of the text column in the dataset. Defaults to "content".
    encoding_sample : float
        The proportion of the data to sample for encoding checks. Defaults to 0.01 (1% of the data).
    random_state : int
        Random seed used for random operations within the ingestion process. Defaults to 22.
    datatypes : dict
        A dictionary that specifies the expected data types for each column in the dataset.
        The default mapping includes:
            - "id": "string"
            - "app_id": "string"
            - "app_name": "string"
            - "category_id": "category"
            - "category": "category"
            - "author": "string"
            - "rating": "int16"
            - "content": "string"
            - "vote_count": "int64"
            - "vote_sum": "int64"
            - "date": "datetime64[ms]"
    """

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
