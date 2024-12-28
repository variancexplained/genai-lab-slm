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
# Modified   : Friday December 27th 2024 10:28:07 pm                                               #
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
from discover.infra.exception.dataset import DatasetIntegrityError
from discover.infra.utils.file.stats import FileStats


# ------------------------------------------------------------------------------------------------ #
#                                  DATA ENVELOPE                                                   #
# ------------------------------------------------------------------------------------------------ #
@dataclass()
class DataEnvelope(DatasetComponent):
    """
    Encapsulates a dataset with its data, file path, structure, and format metadata.

    This class is immutable and ensures data consistency through validation during
    initialization. It supports Pandas and Spark DataFrame structures with metadata
    about file paths and formats.

    Attributes:
        data (Union[pd.DataFrame, DataFrame]): The dataset, either as a Pandas or Spark DataFrame.
        filepath (str): The file path where the dataset is stored or to be stored.
        dftype (Optional[DFType]): The structure of the dataset
            (e.g., PANDAS or SPARK). Defaults to SPARK.
        file_format (Optional[FileFormat]): The file format of the dataset (e.g., PARQUET or CSV).
            Defaults to PARQUET.
        size (Union[int,str]): The size of the DataFrame in bytes as an integer or as a formatted string.
        created (datetime): Datetime the object was created.

    Properties:
        accessed (str): The date the file was last accessed

    Raises:
        TypeError: If the `data` is not a Pandas or Spark DataFrame.
        DatasetIntegrityError: If the `data` and `dftype` are inconsistent.
        ValueError: If the `file_format` is invalid.
    """

    data: Union[pd.DataFrame, DataFrame]
    filepath: str
    dftype: Optional[DFType] = DFType.SPARK
    file_format: Optional[FileFormat] = FileFormat.PARQUET
    size: Optional[Union[int, str]] = None
    created: Optional[datetime] = None

    def __post_init__(self) -> None:
        """
        Validates the consistency of the dataset's structure and format.

        Ensures that:
        - The `data` field is a valid Pandas or Spark DataFrame.
        - The `dftype` matches the type of `data`.
        - The `file_format` is valid.

        Raises:
            TypeError: If the `data` is not a Pandas or Spark DataFrame.
            DatasetIntegrityError: If the `data` and `dftype` are inconsistent.
            ValueError: If the `file_format` is invalid.
        """
        # Validate data structure
        if not isinstance(self.data, (pd.DataFrame, DataFrame)):
            raise TypeError(
                f"Expected a PANDAS or SPARK DataFrame, but received {type(self.data)}"
            )

        # Validate data and dftype consistency
        if not isinstance(self.dftype, DFType):
            raise ValueError(f"Invalid dataframe structure: {self.dftype}")

        # Validate data and dftype consistency
        if isinstance(self.data, pd.DataFrame) and self.dftype != DFType.PANDAS:
            raise DatasetIntegrityError(
                f"Expected PANDAS structure but received {self.dftype.value}."
            )
        if isinstance(self.data, DataFrame) and self.dftype == DFType.PANDAS:
            raise DatasetIntegrityError(
                f"Expected SPARK structure but received {self.dftype.value}."
            )

        # Validate file_format
        if not isinstance(self.file_format, FileFormat):
            raise ValueError(f"Invalid file format: {self.file_format}")

        # Set the create time
        if not self.created:
            self.created = datetime.now()

        # Compute size of the dataset
        if not self.size:
            self.size = FileStats.get_size(path=self.filepath, in_bytes=True)

    @property
    def accessed(self) -> str:
        """The last accessed timestamp of the dataset file."""
        return FileStats.file_last_accessed(filepath=self.filepath)


# ------------------------------------------------------------------------------------------------ #
#                                 DATASET FRAME SPEC                                               #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DataFrameIOSpec(DatasetComponent):
    """
    Represents configuration metadata for a dataset, including file path,
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
    ) -> DataFrameIOSpec:
        return cls(dftype=dftype, filepath=filepath, file_format=file_format)
