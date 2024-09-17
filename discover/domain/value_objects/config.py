#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/domain/value_objects/config.py                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday September 10th 2024 04:49:55 pm                                             #
# Modified   : Monday September 16th 2024 10:20:35 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Abstract Base Class for Data Processing Stage Configurations"""

from dataclasses import dataclass, field
from typing import List

from discover.core.data import DataClass
from discover.domain.value_objects.data_structure import DataStructure
from discover.domain.value_objects.file import FileFormat
from discover.domain.value_objects.lifecycle import Phase, Stage


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DataConfig(DataClass):
    """
    Configuration class for handling data in a pipeline.

    This class defines the configuration settings for handling data in
    various stages of a data preprocessing pipeline. It serves as a base
    configuration class to define attributes necessary for managing data
    repositories, including the stage of the data in the pipeline, its
    name, and its structural definition.

    Attributes:
    -----------
    phase: Phase
        Represents the phase of the app, i.e. DATAPREP, ANALYSIS, or MODELING
    stage : Stage
        Represents the current stage of the data in the pipeline, such as
        INGEST, DQA, and CLEAN.
    name : str
        The name of the dataset being processed. This name typically
        identifies the specific dataset within the pipeline.
    data_structure : DataStructure
        Defines the structure of the data, including the schema or format
        (e.g., column definitions, types) that is expected in each stage
        of the pipeline. Defaults to pandas.

    This class is intended to be extended by other configurations specific
    to data handling, and it provides the core attributes required for
    managing data repositories in a data pipeline.
    """

    phase: Phase
    stage: Stage
    name: str
    data_structure: DataStructure = DataStructure.PANDAS


# ------------------------------------------------------------------------------------------------ #
@dataclass
class SourceDataConfig(DataConfig):
    """
    A subclass of DataConfig that defines the configuration for source data.

    In addition to the attributes inherited from DataConfig, this class adds
    a `format` attribute to specify the file format of the source data, such as
    Parquet or CSV. This configuration is essential for defining how the source
    data should be processed in the pipeline.

    Inherited Attributes:
    ---------------------
    phase: Phase
        Represents the phase of the app, i.e. DATAPREP, ANALYSIS, or MODELING
    stage : Stage
        Represents the current stage of the data in the pipeline, such as
        INGEST, TRANSFORM, or LOAD.
    name : str
        The name of the dataset being processed. This name typically identifies
        the specific dataset within the pipeline.
    data_structure : DataStructure
        Defines the structure of the data, including the schema or format (e.g.,
        column definitions, types) that is expected in each stage of the pipeline.

    Additional Attribute:
    ---------------------
    format : FileFormat
        The file format of the source data. The default is FileFormat.PARQUET_PARTITIONED,
        but it can be set to other formats, such as CSV or JSON. This defines how
        the source data is stored and read in the pipeline.

    This class provides the necessary configuration for handling source data,
    including its format, stage, and structure within the data pipeline.
    """

    format: FileFormat = FileFormat.PARQUET_PARTITIONED


# ------------------------------------------------------------------------------------------------ #
@dataclass
class TargetDataConfig(DataConfig):
    """
    A subclass of DataConfig that defines the configuration for target data.

    In addition to the attributes inherited from DataConfig, this class adds
    a `format` attribute to specify the file format of the target data and
    `partition_cols` to define the columns by which the data should be partitioned.

    Inherited Attributes:
    ---------------------
    phase: Phase
        Represents the phase of the app, i.e. DATAPREP, ANALYSIS, or MODELING
    stage : Stage
        Represents the current stage of the data in the pipeline, such as
        INGEST, TRANSFORM, or LOAD.
    name : str
        The name of the dataset being processed. This name typically identifies
        the specific dataset within the pipeline.
    data_structure : DataStructure
        Defines the structure of the data, including the schema or format (e.g.,
        column definitions, types) that is expected in each stage of the pipeline.

    Additional Attributes:
    ----------------------
    format : FileFormat
        The file format of the target data. The default is FileFormat.PARQUET_PARTITIONED,
        but it can be set to other formats, such as CSV or JSON. This defines how
        the target data is stored and written in the pipeline.

    partition_cols : List[str]
        A list of column names by which the target data should be partitioned. The
        default value is ["category"]. Partitioning helps in organizing and querying
        large datasets by dividing them into smaller, manageable chunks based on
        column values.

    This class provides the necessary configuration for handling target data,
    including its format, partitioning, stage, and structure within the data pipeline.
    """

    format: FileFormat = FileFormat.PARQUET_PARTITIONED
    partition_cols: List[str] = field(default_factory=lambda: ["category"])


# ------------------------------------------------------------------------------------------------ #
@dataclass
class ServiceConfig(DataClass):
    """
    Abstract base class for configuration of data preprocessing stages.

    This class provides configuration details for different stages of data
    preprocessing pipelines, including source and target data configurations,
    and whether the processing should be forced to re-run.

    Attributes:
    -----------
    phase: Phase
        Represents the phase of the app, i.e. DATAPREP, ANALYSIS, or MODELING
    stage : Stage
        Stage of the processing and analysis pipeline.
    target_data_config : DataConfig
        Configuration for the target data where the results of processing will be stored.
    force : bool
        A flag indicating whether the pipeline should forcefully re-process the data,
        even if the output already exists. Defaults to False.
    """

    phase: Phase
    stage: Stage
    force: bool = False
