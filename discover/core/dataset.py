#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/core/dataset.py                                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday August 26th 2024 10:17:42 pm                                                 #
# Modified   : Friday December 27th 2024 06:34:44 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data Structures Module"""
from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional, Union

import pandas as pd
from pyspark.sql import DataFrame

from discover.core.dtypes import IMMUTABLE_TYPES, SEQUENCE_TYPES
from discover.core.file import FileFormat
from discover.infra.exception.dataset import DatasetIntegrityError
from discover.infra.utils.file.stats import FileStats


# ------------------------------------------------------------------------------------------------ #
#                                  DATASET COMPONENTS                                              #
# ------------------------------------------------------------------------------------------------ #
@dataclass()
class DatasetComponents(ABC):
    """Base Class for Data Transfer Objects"""

    def __repr__(self) -> str:
        return "{}({})".format(
            self.__class__.__name__,
            ", ".join(
                "{}={!r}".format(k, v)
                for k, v in self.__dict__.items()
                if type(v) in IMMUTABLE_TYPES
            ),
        )

    def __str__(self) -> str:
        width = 32
        breadth = width * 2
        s = f"\n\n{self.__class__.__name__.center(breadth, ' ')}"
        d = self.as_dict()
        for k, v in d.items():
            if type(v) in IMMUTABLE_TYPES:
                k = k.strip("_")
                s += f"\n{k.rjust(width,' ')} | {v}"
        s += "\n\n"
        return s

    def as_dict(self) -> Dict[str, Union[str, int, float, datetime, None]]:
        """Returns a dictionary representation of the the Config object."""
        return {
            k: self._export_config(v)
            for k, v in self.__dict__.items()
            if not k.startswith("_")
        }

    @classmethod
    def _export_config(
        cls,
        v: Any,
    ) -> Any:
        """Returns v with Configs converted to dicts, recursively."""
        if isinstance(v, IMMUTABLE_TYPES):
            return v
        elif isinstance(v, SEQUENCE_TYPES):
            return type(v)(map(cls._export_config, v))
        elif isinstance(v, dict):
            return v
        elif hasattr(v, "as_dict"):
            return v.as_dict()
        elif isinstance(v, Enum):
            if hasattr(v, "description"):
                return v.description
            else:
                return v.value
        elif isinstance(v, datetime):
            return v.isoformat()
        else:
            return dict()

    def as_df(self) -> Any:
        """Returns the project in DataFrame format"""
        d = self.as_dict()
        return pd.DataFrame(data=d, index=[0])


# ------------------------------------------------------------------------------------------------ #
#                                  DATA ENVELOPE                                                   #
# ------------------------------------------------------------------------------------------------ #
@dataclass()
class DataEnvelope(DatasetComponents):
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
        return FileStats.file_last_accessed(filepath=self._filepath)

    @classmethod
    def from_config(
        cls,
        data: Union[pd.DataFrame, DataFrame],
        data_envelope_config: DataFrameIOSpec,
    ) -> DataEnvelope:
        """
        Creates a `DataEnvelope` instance from the provided data and configuration.

        This method initializes a `DataEnvelope` by combining raw data with the
        configuration metadata specified in a `DataFrameIOSpec` object.

        Args:
            data (Union[pd.DataFrame, DataFrame]): The dataset, either as a Pandas or Spark DataFrame.
            data_envelope_config (DataFrameIOSpec): The configuration object containing
                metadata such as file path, dataframe structure, and file format.

        Returns:
            DataEnvelope: A new `DataEnvelope` instance with the specified data and metadata.
        """
        return cls(
            data=data,
            filepath=data_envelope_config.filepath,
            dftype=data_envelope_config.dftype,
            file_format=data_envelope_config.file_format,
        )
