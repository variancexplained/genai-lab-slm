#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/persist/object/base.py                                              #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 05:39:55 pm                                              #
# Modified   : Monday December 23rd 2024 10:05:03 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""File Access Layer Base Module"""

from abc import ABC, abstractmethod
from typing import Union

import pandas as pd
import pyspark

from discover.asset.dataset.dataset import Dataset

# ------------------------------------------------------------------------------------------------ #
DataFrame = Union[pd.DataFrame, pyspark.sql.DataFrame]


# ------------------------------------------------------------------------------------------------ #
#                               DATA ACCESS OBJECT                                                 #
# ------------------------------------------------------------------------------------------------ #
class DAO(ABC):
    """Abstract base class for File Access Object"""

    @abstractmethod
    def create(self, dataset: Dataset, **kwargs) -> None:
        """Saves the dataset to the data store.
        Args:
            dataset (Dataset): Dataset object
            **kwargs: Arbitrary keyword arguments.
        """

    @abstractmethod
    def read(self, asset_id: str, **kwargs) -> Dataset:
        """Reads the Dataset asset from the data store.

        Args:
            asset_id (str): Asset identifier.
            **kwargs: Arbitrary keyword arguments.
        """

    @abstractmethod
    def exists(self, asset_id: str, **kwargs) -> bool:
        """Evaluates existence of the dataset asset in the data store.

        Args:
            asset_id (str): Asset identifier.
            **kwargs: Arbitrary keyword arguments.
        """

    @abstractmethod
    def delete(self, asset_id: str, **kwargs) -> None:
        """Deletes the dataset from the data store.

        Args:
            asset_id (str): Asset identifier.
            **kwargs: Arbitrary keyword arguments.
        """
