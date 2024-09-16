#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/domain/service/core/data.py                                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday September 13th 2024 07:13:32 pm                                              #
# Modified   : Monday September 16th 2024 12:27:25 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""DataService Module"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from discover.domain.value_objects.config import DataConfig
from discover.domain.value_objects.context import Context


# ------------------------------------------------------------------------------------------------ #
class DataService(ABC):
    """
    The DataService interface defines the contract for obtaining readers and writers
    for data processing. Implementations of this interface should return the appropriate
    Reader and Writer objects based on the provided configuration.

    Methods:
    --------
    get_reader(config: DataConfig) -> Reader:
        Returns an appropriate Reader object for the provided data configuration.

    get_writer(config: DataConfig) -> Writer:
        Returns an appropriate Writer object for the provided data configuration.
    """

    @abstractmethod
    def get_reader(self, config: DataConfig) -> Reader:
        """
        Returns a Reader for the provided configuration.

        Parameters:
        -----------
        config : DataConfig
            A configuration object that defines the environment, stage, format, and data type.

        Returns:
        --------
        Reader
            An object that can read data according to the configuration.
        """
        pass

    @abstractmethod
    def get_writer(self, config: DataConfig) -> Writer:
        """
        Returns a Writer for the provided configuration.

        Parameters:
        -----------
        config : DataConfig
            A configuration object that defines the environment, stage, format, and data type.

        Returns:
        --------
        Writer
            An object that can write data according to the configuration.
        """
        pass


# ------------------------------------------------------------------------------------------------ #
class Reader(ABC):
    """
    The Reader interface defines the contract for reading data. Concrete implementations
    of this interface will handle specific file formats and data formats, such as CSV, Parquet, etc.

    Methods:
    --------
    read() -> Any:
        Reads and returns data from a source.
    """

    @abstractmethod
    def context(self) -> Context:
        """Returns the Reader's context, including class type, name, and Stage"""

    @abstractmethod
    def read(self) -> Any:
        """
        Reads data from the underlying source.

        Returns:
        --------
        Any
            The data read from the source, typically in the format specified by the configuration
            (e.g., a pandas DataFrame, Spark DataFrame, etc.).
        """
        pass


# ------------------------------------------------------------------------------------------------ #
class Writer(ABC):
    """
    The Writer interface defines the contract for writing data. Concrete implementations
    of this interface will handle specific file formats and data formats, such as CSV, Parquet, etc.

    Methods:
    --------
    write(data: Any) -> None:
        Writes data to a target destination.
    """

    @abstractmethod
    def context(self) -> Context:
        """Returns the Reader's context, including class type, name, and Stage"""

    @abstractmethod
    def write(self, data: Any) -> None:
        """
        Writes the provided data to the underlying target.

        Parameters:
        -----------
        data : Any
            The data to be written, typically in a format like pandas DataFrame or Spark DataFrame.

        Returns:
        --------
        None
        """
        pass
