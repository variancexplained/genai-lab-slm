#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/data/dal/base.py                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 05:39:55 pm                                              #
# Modified   : Thursday December 19th 2024 06:16:35 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data Access Layer Base Module"""

import os
import shutil
from abc import ABC, abstractmethod
from typing import Union

import pandas as pd
import pyspark

# ------------------------------------------------------------------------------------------------ #
DataFrame = Union[pd.DataFrame, pyspark.sql.DataFrame]


# ------------------------------------------------------------------------------------------------ #
class DataFrameReader(ABC):
    """Abstract base class for the data readers."""

    @abstractmethod
    def read(self, filepath, **kwargs) -> DataFrame:
        """Reads data from the designated filepath
        Args:
            filepath (str): Path to file.
            **kwargs: Arbitrary keyword arguments.
        """


# ------------------------------------------------------------------------------------------------ #
class DataFrameWriter(ABC):
    """Abstract base class for the data readers."""

    @abstractmethod
    def write(self, data: DataFrame, filepath: str, **kwargs) -> None:
        """Writes data to the designated filepath
        Args:
            data (DataFrame): Data to write
            filepath (str): Path to file.
            **kwargs: Arbitrary keyword arguments.
        """


# ------------------------------------------------------------------------------------------------ #
class DAO(ABC):

    def read(self, *args, **kwargs) -> DataFrame:
        """Reads data from the designated filepath
        Args:
            *args. Arbitrary positional arguments.
            **kwargs: Arbitrary keyword arguments.
        """
        return self._reader(filepath=filepath)

    def create(self, data: DataFrame, filepath: str, **kwargs) -> None:
        """Writes data to the designated filepath
        Args:
            data (DataFrame): Data to write
            filepath (str): Path to file.
            **kwargs: Arbitrary keyword arguments.
        """
        self._write(data=data, filepath=filepath, **kwargs)

    def exists(self, filepath: str, **kwargs) -> None:
        """Checks existence of data at the filepath

        Args:
            filepath (str): Path to file.
            **kwargs: Arbitrary keyword arguments.
        """
        return os.path.exists(filepath)

    def delete(self, filepath: str, **kwargs) -> None:
        """Deletes the designated file.

        Args:
            filepath (str): Path to file.
            **kwargs: Arbitrary keyword arguments.
        """
        if os.path.isfile(filepath):
            os.remove(filepath)
        elif os.path.isdir(filepath):
            shutil.rmtree(filepath)
