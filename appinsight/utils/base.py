#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/utils/base.py                                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday May 29th 2024 12:30:18 am                                                 #
# Modified   : Wednesday June 5th 2024 04:52:36 am                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Utils Base Class"""
import logging
import os
from abc import ABC, abstractmethod
from typing import Any, Union

import pandas as pd
from dotenv import load_dotenv
from pyspark.sql import DataFrame

from appinsight.utils.env import EnvManager
from appinsight.utils.repo import DatasetRepo


# ------------------------------------------------------------------------------------------------ #
class Converter(ABC):
    """Abstract base class for dataframe converters."""

    def __init__(self, efm_cls: type[EnvManager] = EnvManager) -> None:
        load_dotenv()
        self.tempdir = os.getenv(key="TEMPDIR")

        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @abstractmethod
    def convert(
        self, data: Union[pd.DataFrame, DataFrame], *args, **kwargs
    ) -> Union[pd.DataFrame, DataFrame]:
        """Abstract method for methods that perform the dataframe conversion

        Args:
            data (Union[pd.DataFrame, DataFrame]): Data to be converted.
        """


# ------------------------------------------------------------------------------------------------ #
class Reader(ABC):
    """Abstract base class for reader utilities.

    Args:
        dsm (DatasetRepo): Dataset Manager responsible for files in environments.
    """

    def __init__(self, dsm: DatasetRepo = DatasetRepo, **kwargs) -> None:
        super().__init__()
        self.dsm = dsm
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @abstractmethod
    def read(self, directory: str, filename: str, **kwargs) -> Any:
        """Abstract method defining the method signature for read operations."""


# ------------------------------------------------------------------------------------------------ #
class Writer(ABC):
    """Abstract base class for writer utilities.

    Args:
        dsm (DatasetRepo): Dataset Manager responsible for files in environments.
    """

    def __init__(self, dsm: DatasetRepo = DatasetRepo, **kwargs) -> None:
        super().__init__()
        self.dsm = dsm
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @abstractmethod
    def write(self, data: Any, directory: str, filename: str, **kwargs) -> None:
        """Abstract method defining the method signature for write operations."""
