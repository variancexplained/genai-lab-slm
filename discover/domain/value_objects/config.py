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
# Modified   : Monday September 16th 2024 12:27:08 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Abstract Base Class for Data Processing Stage Configurations"""

from dataclasses import dataclass, field
from typing import List

from discover.core.data import DataClass
from discover.domain.value_objects.config import DataStructure
from discover.domain.value_objects.file_format import FileFormat
from discover.domain.value_objects.lifecycle import Stage

# ------------------------------------------------------------------------------------------------ #


@dataclass
class DataConfig(DataClass):
    """
    Configuration class for data handling.

    This class defines the configuration for data repositories used in the
    data preprocessing pipeline. It includes the repository, stage, and
    the name of the data being processed.

    Attributes:
    -----------
    repo : Repo
        Repository object responsible for data storage and retrieval.
    stage : Stage
        The stage of the data in the pipeline (e.g., INGEST, TRANSFORM).
    name : str
        The name of the data being processed.
    """

    stage: Stage
    name: str
    data_structure: DataStructure
    format: FileFormat
    partition_cols: List[str] = field(default_factory=list)


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
    stage : Stage
        Stage of the processing and analysis pipeline.
    target_data_config : DataConfig
        Configuration for the target data where the results of processing will be stored.
    force : bool
        A flag indicating whether the pipeline should forcefully re-process the data,
        even if the output already exists. Defaults to False.
    """

    stage: Stage
    force: bool = False
