#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/domain/entity/config/dataset.py                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday September 10th 2024 04:49:55 pm                                             #
# Modified   : Thursday September 19th 2024 09:10:02 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Abstract Base Class for Data Processing Stage Configurations"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import List, Optional

from discover.domain.entity.config.base import Config
from discover.domain.exception.config import InvalidConfigException
from discover.domain.value_objects.data_structure import DataStructure
from discover.domain.value_objects.file import ExistingDataBehavior, FileFormat
from discover.domain.value_objects.lifecycle import Stage


# ------------------------------------------------------------------------------------------------ #
#                                  DATASET CONFIG                                                  #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatasetConfig(Config):
    """
    Configuration class for managing the dataset's structure, format, and behavior in a data pipeline.

    This class defines various configurations required for reading or writing data in different stages
    of the pipeline, such as dataset structure, file format, and behavior when encountering existing data.

    Attributes:
    -----------
    stage : Stage
        The specific stage of the pipeline where the data will be read or written (e.g., INGEST, TRANSFORM).

    name : str
        The name of the dataset being processed or created.

    data_structure : DataStructure, default=DataStructure.PANDAS
        Defines the structure of the dataset, such as whether it is a Pandas DataFrame or another data structure
        (e.g., Spark DataFrame).

    format : FileFormat, default=FileFormat.PARQUET_PARTITIONED
        Specifies the file format of the dataset, e.g., Parquet partitioned or other formats.

    partition_cols : List[str], default=[]
        The list of columns by which the dataset is partitioned when saved, primarily for use with partitioned
        file formats like Parquet.

    existing_data_behavior : Optional[ExistingDataBehavior], default=None
        Defines the behavior when encountering existing data, such as whether to overwrite or delete
        matching records.

    Methods:
    --------
    __post_init__() -> None:
        A post-initialization hook that sets default values for `partition_cols` and `existing_data_behavior`
        when the file format is `PARQUET_PARTITIONED`. Ensures that the partitioning behavior is applied
        correctly when working with partitioned datasets.

    validate() -> None:
        Validates the configuration by checking that `stage`, `name`, `data_structure`, `format`,
        `partition_cols`, and `existing_data_behavior` (if provided) are correctly set. Inherits the base
        validation from `Config` and raises an `InvalidConfigException` if any attribute is invalid.
    """

    stage: Stage  # The Stage to/from the data will be written/read.
    name: str  # The name of the dataset
    data_structure: DataStructure = DataStructure.PANDAS  # The dataset data structure.
    format: FileFormat = FileFormat.PARQUET_PARTITIONED  # The dataset file format.
    partition_cols: List[str] = field(default_factory=list)
    existing_data_behavior: Optional[str] = None

    def __post_init__(self) -> None:
        """
        A post-initialization hook that sets default values for `partition_cols` and `existing_data_behavior`
        when the file format is `PARQUET_PARTITIONED`. This ensures that partitioning columns and data behavior
        are applied correctly when working with partitioned datasets.
        """
        if self.format == FileFormat.PARQUET_PARTITIONED:
            self.partition_cols = ["category"]
            self.existing_data_behavior = ExistingDataBehavior.DELETE_MATCHING.value

    def validate(self) -> None:
        """
        Validates the DatasetConfig.

        Inherits the base validation from `Config` to ensure the service context is valid. Additionally, this method
        checks:
        - `stage` is an instance of `Stage`.
        - `name` is a string.
        - `data_structure` is an instance of `DataStructure`.
        - `format` is an instance of `FileFormat`.
        - `partition_cols` is a list.
        - `existing_data_behavior` is either `None` or an instance of `ExistingDataBehavior`.

        If any of these checks fail, an `InvalidConfigException` is raised with a detailed error message.
        """
        super().validate()
        errors = []
        if not isinstance(self.stage, Stage):
            errors.append(
                f"Invalid {self.__class__.__name__}. Expected a Stage instance. Encountered {type(self.stage).__name__}."
            )
        if not isinstance(self.name, str):
            errors.append(
                f"Invalid {self.__class__.__name__}. Expected a string for name. Encountered {type(self.name).__name__}."
            )
        if not isinstance(self.data_structure, DataStructure):
            errors.append(
                f"Invalid {self.__class__.__name__}. Expected a DataStructure instance. Encountered {type(self.data_structure).__name__}."
            )
        if not isinstance(self.format, FileFormat):
            errors.append(
                f"Invalid {self.__class__.__name__}. Expected a FileFormat instance. Encountered {type(self.format).__name__}."
            )
        if not isinstance(self.partition_cols, list):
            errors.append(
                f"Invalid {self.__class__.__name__}. Expected a list for partition_cols. Encountered {type(self.partition_cols).__name__}."
            )
        if self.existing_data_behavior and not isinstance(
            self.existing_data_behavior, ExistingDataBehavior
        ):
            errors.append(
                f"Invalid {self.__class__.__name__}. Expected an ExistingDataBehavior instance. Encountered {type(self.existing_data_behavior).__name__}."
            )

        if errors:
            error_msg = "\n".join(errors)
            logging.error(error_msg)
            raise InvalidConfigException(error_msg)
