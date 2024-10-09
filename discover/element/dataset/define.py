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
# Modified   : Tuesday October 8th 2024 09:16:31 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Module"""
from __future__ import annotations

from dataclasses import dataclass

from discover.element.base.define import Element


# ------------------------------------------------------------------------------------------------ #
@dataclass
class Dataset(Element):
    """
    Represents a dataset, inheriting from the `Element` class.

    In addition to the attributes from `Element`, the `Dataset` class tracks
    the number of rows, columns, and overall size of the dataset.

    Inherited Attributes from `Element`:
        id (int): The unique identifier of the dataset.
        name (str): The name of the dataset.
        phase (PhaseDef): Defines the current phase of the dataset.
        stage (StageDef): Defines the current stage of the dataset.
        content (Any): The payload or data associated with the dataset.
        storage_config (StorageConfig): Configuration for storing the dataset.
        description (Optional[str]): A description of the dataset. Default is None.
        cost (float): The duration in seconds between the creation and persistence of the dataset. Default is 0.0.
        created (Optional[datetime]): The timestamp when the dataset was created. Default is None.
        persisted (Optional[datetime]): The timestamp when the dataset was persisted. Default is None.

    Attributes:
        nrows (int): The number of rows in the dataset. Default is 0.
        ncols (int): The number of columns in the dataset. Default is 0.
        size (float): The size of the dataset (e.g., in MB or GB). Default is 0.
    """

    nrows: int = 0
    ncols: int = 0
    size: float = 0

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
