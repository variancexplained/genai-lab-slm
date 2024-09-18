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
# Modified   : Wednesday September 18th 2024 05:10:23 pm                                           #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Abstract Base Class for Data Processing Stage Configurations"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from discover.core.data import DataClass
from discover.domain.value_objects.data_structure import DataStructure
from discover.domain.value_objects.file import FileFormat
from discover.domain.value_objects.lifecycle import Phase, Stage


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DataConfig(DataClass):
    """
    Configuration class for handling data-related tasks within a pipeline.

    This class defines various configurations necessary for processing data in different stages
    of the pipeline, such as lifecycle phases, dataset structure, file formats, and partitioning options.
    It also manages behaviors related to existing data when processing or writing datasets.

    Attributes:
    -----------
    phase : Phase
        The lifecycle phase, e.g., DATAPREP, ANALYSIS, or GENAI, indicating the current stage
        of the data processing lifecycle.

    stage : Stage
        The specific stage of the service pipeline (e.g., DQA, CLEAN, FEATURE) during which
        this configuration applies.

    io_stage : Stage
        The stage from which data is read or to which data is written. This represents input/output
        behavior within the data flow.

    name : str
        The name of the dataset being processed or created.

    datatypes : Dict[str, Any], default={}
        A dictionary mapping data columns to their respective data types, allowing for structured
        data handling during processing.

    data_structure : DataStructure, default=DataStructure.PANDAS
        Defines the structure of the dataset, such as whether it is a Pandas DataFrame or another
        data structure (e.g., Spark DataFrame).

    format : FileFormat, default=FileFormat.PARQUET_PARTITIONED
        Specifies the file format of the dataset, e.g., Parquet partitioned or other formats.

    partition_cols : List[str], default=[]
        Columns by which the dataset is partitioned when saved, used primarily for partitioned file
        formats like Parquet.

    existing_data_behavior : Optional[str], default=None
        Defines the behavior when existing data is encountered, such as whether to overwrite or delete
        matching records.

    Methods:
    --------
    __post_init__() -> None:
        A post-initialization hook that sets default values for `partition_cols` and `existing_data_behavior`
        if the file format is `PARQUET_PARTITIONED`. Ensures that partitioning is applied correctly based
        on the dataset format.
    """

    phase: Phase  # Lifecycle phase, i.e. DATAPREP, ANALYSIS, or GENAI
    stage: Stage  # The Stage to/from the data will be written/read.
    name: str  # The name of the dataset
    data_structure: DataStructure = DataStructure.PANDAS  # The dataset data structure.
    format: FileFormat = FileFormat.PARQUET_PARTITIONED  # The dataset file format.
    partition_cols: List[str] = field(default_factory=list)
    existing_data_behavior: Optional[str] = None

    def __post_init__(self) -> None:
        if self.format == FileFormat.PARQUET_PARTITIONED:
            self.partition_cols: List[str] = field(default_factory=lambda: ["category"])
            self.existing_data_behavior: str = "delete_matching"


# ------------------------------------------------------------------------------------------------ #
@dataclass
class ServiceConfig(DataClass):
    """
    Configuration data class for service-related tasks.

    This class holds the configuration details required to manage service tasks, including
    source and target data configurations, the current phase and stage of the pipeline,
    and additional flags that control service behavior.

    Attributes:
    -----------
    source_data_config : DataConfig
        Configuration object for the source data, providing details such as data structure,
        file paths, or database connection info.

    target_data_config : DataConfig
        Configuration object for the target data, specifying where the processed data should
        be stored or how it should be handled after processing.

    phase : Phase
        The current phase of the pipeline or workflow (e.g., extract, transform, load).

    stage : Stage
        The specific stage within the phase that the service is working on. Describes which
        step in the pipeline the configuration is applicable to.

    force : bool, default=False
        A flag indicating whether to force the execution of the task, potentially overriding
        conditions like caching or skipping steps that have already been completed.
    """

    source_data_config: DataConfig
    target_data_config: DataConfig
    phase: Phase
    stage: Stage
    force: bool = False
