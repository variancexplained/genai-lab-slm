#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/asset/dataset/iospec.py                                                   #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday December 27th 2024 06:23:18 pm                                               #
# Modified   : Friday December 27th 2024 08:18:41 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from __future__ import annotations

from enum import Enum

from pydantic.dataclasses import dataclass
from dependency_injector.wiring import inject
from discover.asset.base import AssetBuilder
from discover.asset.dataset.build import DatasetBuilder
from discover.core.dataset import DatasetComponents


# ------------------------------------------------------------------------------------------------ #
#                                     FILE FORMATS                                                 #
# ------------------------------------------------------------------------------------------------ #
class FileFormat(Enum):
    CSV = "csv"
    PARQUET = "parquet"


# ------------------------------------------------------------------------------------------------ #
#                                  DATAFRAME LIBRARY                                               #
# ------------------------------------------------------------------------------------------------ #
class DFType(Enum):

    PANDAS = "pandas"
    SPARK = "spark"
    SPARKNLP = "sparknlp"


# ------------------------------------------------------------------------------------------------ #
#                                 DATASET FRAME SPEC                                               #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DataFrameIOSpec(DatasetComponents):
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

    filepath: str
    dftype: DFType
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
    @inject
    def create(cls, asset_id: str, dftype: DFType, file_format: FileFormat, workspace_location: str, file_location: str) -> DataFrameIOSpec:


# ------------------------------------------------------------------------------------------------ #
#                             DATAFRAME SPEC BUILDER                                               #
# ------------------------------------------------------------------------------------------------ #


class DataFrameIOSpecBuilder(AssetBuilder):
    """
    Abstract base class for building assets with phases, stages, and persistence
    configurations.
    """
    def __init__(self, dataset_builder: DatasetBuilder):
        self._dataset_builder = dataset_builder
        self._dftype = None
        self._file_format = None
        self._filepath = None

    def reset(self) -> None:
        """
        Resets the builder to be ready to construct another Dataset object.
        """
        self._dftype = None
        self._file_format = None
        self._filepath = None

    def pandas(self) -> DataFrameIOSpecBuilder:
        self._dftype = DFType.PANDAS
        return self

    def file_format(self, file_format: FileFormat) -> DataFrameIOSpecBuilder:
        self._file_format = file_format
        return self

    def build(self) -> Dataset:
        """
        Builds and returns the final Dataset object based on the provided configurations.

        Returns:
            Dataset: The fully constructed dataset.
        """
        pass
