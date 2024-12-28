#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/asset/dataset/component/data.py                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday December 27th 2024 08:32:52 pm                                               #
# Modified   : Saturday December 28th 2024 02:43:49 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Data Module"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Union

import pandas as pd
from pyspark.sql import DataFrame

from discover.asset.dataset import DFType, FileFormat
from discover.asset.dataset.base import DatasetComponent
from discover.infra.utils.file.stats import FileStats


# ------------------------------------------------------------------------------------------------ #
#                                    DATA SOURCE                                                   #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DataSource(DatasetComponent):
    data: Union[pd.DataFrame, DataFrame]
    dtype: DFType


# ------------------------------------------------------------------------------------------------ #
#                                    FILE SOURCE                                                   #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class FileSource(DatasetComponent):
    dftype: DFType
    filepath: str
    file_format: FileFormat


# ------------------------------------------------------------------------------------------------ #
#                                   DATA ENVELOPE                                                  #
# ------------------------------------------------------------------------------------------------ #
@dataclass()
class DataEnvelope(DatasetComponent):

    data: Union[pd.DataFrame, DataFrame]
    filepath: str
    dftype: DFType
    file_format: FileFormat
    created: datetime
    size: Optional[int] = None

    def __post_init__(self) -> None:

        # Compute size of the dataset
        if not self.size:
            self.size = FileStats.get_size(path=self.filepath, in_bytes=True)

    @property
    def accessed(self) -> str:
        """The last accessed timestamp of the dataset file."""
        return FileStats.file_last_accessed(filepath=self.filepath)


# ------------------------------------------------------------------------------------------------ #
#                                DATAFRAME FILE CONFIG                                             #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DataFrameFileConfig(DatasetComponent):
    """
    Represents configuration metadata for a dataset file, including file path,
    structure type, and file format.

    This class provides validation during initialization to ensure that
    the provided metadata is consistent and valid.

    Attributes:
        filepath (str): The path to the file associated with the dataset.
        dftype (DFType): The structure of the dataset
            (e.g., PANDAS or SPARK).
        file_format (FileFormat): The format of the file (e.g., PARQUET, CSV).

    Raises:
        ValueError: If `dftype` or `file_format` is invalid.
    """

    dftype: DFType
    filepath: str
    file_format: FileFormat

    def __post_init__(self) -> None:
        """
        Validates the configuration metadata.

        Ensures that:
        - `dftype` is a valid instance of `DFType`.
        - `file_format` is a valid instance of `FileFormat`.

        Raises:
            ValueError: If `dftype` is not a valid `DFType`.
            ValueError: If `file_format` is not a valid `FileFormat`.
        """
        # Validate dftype
        if not isinstance(self.dftype, DFType):
            raise ValueError(f"Invalid dataframe structure: {self.dftype}")

        # Validate file_format
        if not isinstance(self.file_format, FileFormat):
            raise ValueError(f"Invalid file format: {self.file_format}")

    @classmethod
    def create(
        cls, dftype: DFType, file_format: FileFormat, filepath: str
    ) -> DataFrameFileConfig:
        return cls(dftype=dftype, filepath=filepath, file_format=file_format)
