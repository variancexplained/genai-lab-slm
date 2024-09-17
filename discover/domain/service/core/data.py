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
# Modified   : Tuesday September 17th 2024 03:23:32 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""DataService Module"""
from __future__ import annotations

import hashlib
from abc import ABC, abstractmethod
from typing import Any, Union

import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame

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

    def __init__(self, config: DataConfig) -> None:
        self._context = Context(
            process_type="Reader",
            process_name=self.__class__.__name__,
            stage=config.stage,
        )

    def context(self) -> Context:
        """Returns the Reader's context, including class type, name, and Stage"""
        return self._context

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

    def __init__(self, config: DataConfig) -> None:
        self._context = Context(
            process_type="Writer",
            process_name=self.__class__.__name__,
            stage=config.stage,
        )

    def context(self) -> Context:
        """Returns the Writer's context, including class type, name, and Stage"""
        return self._context

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


# ------------------------------------------------------------------------------------------------ #
def find_dataframe(args, kwargs) -> Union[pd.DataFrame, SparkDataFrame]:
    """
    Searches for a pandas or Spark DataFrame in the positional and keyword arguments.

    Parameters:
    -----------
    args : tuple
        The positional arguments of the function.
    kwargs : dict
        The keyword arguments of the function.

    Returns:
    --------
    df : pandas.DataFrame or pyspark.sql.DataFrame or None
        Returns the found DataFrame, or None if no DataFrame is found.
    """
    df = None
    # Search through args first for pandas or Spark DataFrame
    for arg in args:
        if isinstance(arg, pd.DataFrame):
            df = arg
            break
        elif isinstance(arg, SparkDataFrame):
            df = arg
            break

    # If no DataFrame found in args, search through kwargs
    if df is None:
        for key, value in kwargs.items():
            if isinstance(value, pd.DataFrame):
                df = value
                break
            elif isinstance(value, SparkDataFrame):
                df = value
                break
    return df


# ------------------------------------------------------------------------------------------------ #
def hash_dataframe(
    df: Union[pd.DataFrame, SparkDataFrame], hash_length: int = 8
) -> str:
    """
    Generates a fixed-length hash based on the dimensions, schema, and optionally sampled data of a pandas or Spark DataFrame.

    This function creates a hash from the number of rows, number of columns, and schema of the DataFrame. Optionally,
    it includes sampled data from the DataFrame for increased uniqueness when content changes.

    Parameters:
    -----------
    df : Union[pd.DataFrame, pyspark.sql.DataFrame]
        The DataFrame for which to generate a hash. Can be either a pandas DataFrame or a Spark DataFrame.
    hash_length : int, optional
        The length of the hash to be generated (default is 8 characters).

    Returns:
    --------
    str
        The generated hash as a string of the specified length.

    Raises
    ------
    ValueError: On empty Dataframe

    Example:
    --------
    >>> import pandas as pd
    >>> df = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
    >>> hash_dataframe(df)
    '9a83f5ac'

    >>> from pyspark.sql import SparkSession
    >>> spark = SparkSession.builder.appName("example").getOrCreate()
    >>> sdf = spark.createDataFrame([(1, 2), (3, 4)], ["col1", "col2"])
    >>> hash_dataframe(sdf)
    'e3b0c442'
    """

    # Handle empty DataFrame case
    if isinstance(df, (pd.DataFrame, pd.core.frame.DataFrame)) and df.empty:
        raise ValueError("Encountered an empty dataframe")
    elif isinstance(df, SparkDataFrame) and df.count() == 0:
        raise ValueError("Encountered an empty dataframe")

    # Check if it's a pandas DataFrame
    if isinstance(df, (pd.DataFrame, pd.core.frame.DataFrame)):
        num_rows = len(df)
        num_columns = len(df.columns)
        schema_str = str(df.dtypes.values)
    else:
        num_rows = df.count()
        num_columns = len(df.columns)
        schema_str = df.schema.simpleString()

    # Combine the dimensions, schema, and data for hashing
    dimensions_str = f"{num_rows}-{num_columns}-{schema_str}"

    # Generate a hash from the dimensions, schema, and sample data
    hasher = hashlib.blake2s(digest_size=hash_length)
    hasher.update(dimensions_str.encode("utf-8"))

    return hasher.hexdigest()
