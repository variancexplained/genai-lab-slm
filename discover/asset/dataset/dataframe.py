#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/asset/dataset/dataframe.py                                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday December 23rd 2024 06:26:32 pm                                               #
# Modified   : Monday December 23rd 2024 06:42:38 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from __future__ import annotations

from enum import Enum

from discover.infra.persist.dataframe.base import DataFrameReader, DataFrameWriter
from discover.infra.persist.dataframe.pandas import (
    DataFrameReader as PandasDataFrameReader,
)
from discover.infra.persist.dataframe.pandas import (
    DataFrameWriter as PandasDataFrameWriter,
)
from discover.infra.persist.dataframe.spark import (
    DataFrameReader as SparkDataFrameReader,
)
from discover.infra.persist.dataframe.spark import (
    DataFrameWriter as SparkDataFrameWriter,
)


# ------------------------------------------------------------------------------------------------ #
class DataFrameStructure(Enum):
    """
    Enum representing different DataFrame structures and their associated attributes.

    Each DataFrame structure is defined by:
        - A string identifier.
        - A flag indicating support for distributed computing.
        - A flag indicating support for NLP-specific functionality.
        - A reader class for reading data into the structure.
        - A writer class for writing data from the structure.

    Attributes:
        distributed (bool): Whether the DataFrame type supports distributed computing.
        nlp (bool): Whether the DataFrame type supports NLP-specific functionality.
        reader (DataFrameReader): The class responsible for reading data into the structure.
        writer (DataFrameWriter): The class responsible for writing data from the structure.
    """

    PANDAS = ("pandas", False, False, PandasDataFrameReader, PandasDataFrameWriter)
    SPARK = ("spark", True, False, SparkDataFrameReader, SparkDataFrameWriter)
    SPARKNLP = ("sparknlp", True, True, SparkDataFrameReader, SparkDataFrameWriter)

    def __new__(
        cls,
        value: str,
        distributed: bool,
        nlp: bool,
        reader: DataFrameReader,
        writer: DataFrameWriter,
    ) -> DataFrameStructure:
        """
        Creates a new DataFrameStructure instance.

        Args:
            value (str): The string identifier of the DataFrame structure (e.g., "pandas").
            distributed (bool): Whether the structure supports distributed computing.
            nlp (bool): Whether the structure supports NLP-specific functionality.
            reader (DataFrameReader): The class responsible for reading data.
            writer (DataFrameWriter): The class responsible for writing data.

        Returns:
            DataFrameStructure: A new instance of the DataFrameStructure enum.
        """
        obj = object.__new__(cls)
        obj._value_ = value
        obj._distributed = distributed
        obj._nlp = nlp
        obj._reader = reader
        obj._writer = writer
        return obj

    @property
    def distributed(self) -> bool:
        """
        Indicates if the DataFrame type supports distributed computing.

        Returns:
            bool: True if the structure supports distributed computing, False otherwise.
        """
        return self._distributed

    @property
    def nlp(self) -> bool:
        """
        Indicates if the DataFrame type supports NLP-specific functionality.

        Returns:
            bool: True if the structure supports NLP-specific functionality, False otherwise.
        """
        return self._nlp

    @property
    def reader(self) -> DataFrameReader:
        """
        Returns the reader class associated with the DataFrame structure.

        Returns:
            DataFrameReader: The reader class for the structure.
        """
        return self._reader

    @property
    def writer(self) -> DataFrameWriter:
        """
        Returns the writer class associated with the DataFrame structure.

        Returns:
            DataFrameWriter: The writer class for the structure.
        """
        return self._writer

    @classmethod
    def from_value(cls, value: str) -> DataFrameStructure:
        """
        Finds the enum member based on its string identifier.

        Args:
            value (str): The string identifier of the DataFrame structure (e.g., "pandas").

        Returns:
            DataFrameStructure: The matching enum member.

        Raises:
            ValueError: If no matching enum member is found.
        """
        value = value.lower()
        for member in cls:
            if member._value_ == value:
                return member
        raise ValueError(f"No matching {cls.__name__} for {value}")
