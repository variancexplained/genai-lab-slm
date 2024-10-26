#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/assets/dataset.py                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 01:35:04 am                                              #
# Modified   : Saturday October 26th 2024 12:05:21 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Module"""
from __future__ import annotations

from typing import Any, Optional

import pandas as pd
from pydantic.dataclasses import dataclass

from discover.assets.base import Asset


# ------------------------------------------------------------------------------------------------ #
@dataclass
class Dataset(Asset):
    """
    Represents a dataset, encapsulating content along with metadata like
    the number of rows, columns, and size.

    Inherits from:
        Asset: Base class that provides phase, stage, content, and storage
        configuration information.

    Attributes:
        nlp (bool): Whether the dataset is part of an NLP pipeline
        distributed (bool): Whether the dataset contents are persisted in a distributed dataframe structure.
        storage_location (Any): Object specifying the storage location for the dataset content payload.
        nrows (int): The number of rows in the dataset. Defaults to 0.
        ncols (int): The number of columns in the dataset. Defaults to 0.
        size (float): The size of the dataset in bytes. Defaults to 0.

    Methods:
        __post_init__() -> None:
            Initializes the `nrows`, `ncols`, and `size` attributes based on
            the type of content provided. If the content is a Spark DataFrame,
            it calculates the number of rows and columns using Spark methods.
            Otherwise, it uses pandas methods for DataFrames.

        __eq__(other: object) -> bool:
            Checks for equality between two `Dataset` instances. Two datasets
            are considered equal if they have the same number of rows, columns,
            and size.
    """

    nlp: bool = False
    distributed: bool = False
    storage_location: Optional[Any] = None
    nrows: int = 0
    ncols: int = 0
    size: float = 0

    def __post_init__(self) -> None:
        """
        Initializes the dataset's `nrows`, `ncols`, and `size` attributes.

        If the content is a Spark DataFrame, it computes the number of rows
        using `count()` and the number of columns from the DataFrame's
        `columns` attribute. The size is calculated by mapping each row to its
        string representation and summing their lengths.

        If the content is a pandas DataFrame, it computes the number of rows
        using `shape[0]`, the number of columns using `shape[1]`, and the size
        using the `memory_usage(deep=True).sum()` method.

        Raises:
            AttributeError: If the `content` does not have the expected attributes
            for row count, column count, or size calculation.
        """
        super().__post_init__()
        if isinstance(self.content, (pd.DataFrame, pd.core.frame.DataFrame)):
            self.nrows = self.content.shape[0]
            self.ncols = self.content.shape[1]
            self.size = self.content.memory_usage(deep=True).sum()
        else:
            self.nrows = self.content.count()
            self.ncols = len(self.content.columns)
            self.size = self.content.count()

    def __eq__(self, other: object) -> bool:
        """
        Checks for equality between two `Dataset` instances.

        Two datasets are considered equal if they have the same number of rows,
        columns, and size. This method overrides the equality operator to facilitate
        direct comparison between `Dataset` objects.

        Args:
            other (object): The object to compare against.

        Returns:
            bool: True if the datasets have the same number of rows, columns, and size;
                  otherwise, False.
        """
        if not isinstance(other, Dataset):
            return NotImplemented
        return (
            self.nrows == other.nrows
            and self.ncols == other.ncols
            and self.size == other.size
        )
