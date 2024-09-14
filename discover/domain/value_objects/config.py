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
# Modified   : Saturday September 14th 2024 05:49:13 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Abstract Base Class for Data Processing Stage Configurations"""

from dataclasses import dataclass

from discover.core.data import DataClass
from discover.domain.base.repo import Repo
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

    repo: Repo
    stage: Stage
    name: str


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
    source_data_config : DataConfig
        Configuration for the source data being processed in the pipeline.
    target_data_config : DataConfig
        Configuration for the target data where the results of processing will be stored.
    force : bool
        A flag indicating whether the pipeline should forcefully re-process the data,
        even if the output already exists. Defaults to False.
    """

    source_data_config: DataConfig
    target_data_config: DataConfig
    force: bool = False
