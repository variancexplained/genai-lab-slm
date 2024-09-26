#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/element/dataset/define.py                                                 #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 01:35:04 am                                              #
# Modified   : Wednesday September 25th 2024 10:44:01 pm                                           #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Module"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Union

import pandas as pd
import pyspark

from discover.core.flow import PhaseDef, StageDef
from discover.element.base.define import Element, ElementMetadata
from discover.element.dataset.store import DatasetStorageConfig


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatasetMetadata(ElementMetadata):
    """
    Represents metadata for a dataset in the system.

    Inherits from `ElementMetadata` and adds attributes specific to datasets.

    Attributes:
        nrows (int): The number of rows in the dataset.
        ncols (int): The number of columns in the dataset.
        size (float): The size of the dataset in bytes.
    """

    nrows: int = 0
    ncols: int = 0
    size: float = 0

    @classmethod
    def create(
        cls,
        id: int,
        name: str,
        phase: PhaseDef,
        stage: StageDef,
        nrows: int,
        ncols: int,
        size: float,
        element_type: str,
    ) -> DatasetMetadata:
        """
        Creates a new instance of `DatasetMetadata`.

        Args:
            id (int): Unique identifier for the dataset.
            name (str): Name of the dataset.
            phase (PhaseDef): Phase in which the dataset is created.
            stage (StageDef): Stage in which the dataset resides.
            nrows (int): The number of rows in the dataset.
            ncols (int): The number of columns in the dataset.
            size (float): The size of the dataset in bytes.
            element_type (str): Type of element. Usually class name.

        Returns:
            DatasetMetadata: A new instance of `DatasetMetadata`.
        """
        return cls(
            id=id,
            name=name,
            phase=phase,
            stage=stage,
            nrows=nrows,
            ncols=ncols,
            size=size,
            element_type=element_type,
        )


# ------------------------------------------------------------------------------------------------ #
@dataclass
class Dataset(Element):
    """
    Represents a dataset, including its content and metadata.

    Attributes:
        content (Union[pd.DataFrame, pyspark.sql.DataFrame]): The actual data of the dataset, which can be either a Pandas or Spark DataFrame.
        metadata (DatasetMetadata): Metadata associated with the dataset.
        storage_config (DatasetStorageConfig): Configuration for how the dataset's data is stored.
    """

    content: Union[pd.DataFrame, pyspark.sql.DataFrame]
    metadata: DatasetMetadata
    storage_config: DatasetStorageConfig

    def __eq__(self, other: object) -> bool:
        """
        Checks for equality between two Dataset instances.

        Args:
            other (object): The object to compare against.

        Returns:
            bool: True if the datasets have the same number of rows, columns, and size; otherwise, False.
        """
        if not isinstance(other, Dataset):
            return NotImplemented
        return (
            self.metadata.nrows == other.metadata.nrows
            and self.metadata.ncols == other.metadata.ncols
            and self.metadata.size == other.metadata.size
        )
