#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/core/data_structure.py                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday August 26th 2024 10:17:42 pm                                                 #
# Modified   : Sunday December 15th 2024 01:00:03 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Core Data Structures """
from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from types import SimpleNamespace
from typing import Any, Dict, Mapping, Tuple, Union

import numpy as np
import pandas as pd
from dependency_injector.providers import ConfigurationOption


# ------------------------------------------------------------------------------------------------ #
class NestedNamespace(SimpleNamespace):
    def __init__(
        self, dictionary: Union[Mapping[str, Union[int, float]], ConfigurationOption]
    ) -> None:
        super().__init__()
        for key, value in dictionary.items():
            if isinstance(value, dict):
                self.__setattr__(key, NestedNamespace(value))
            else:
                self.__setattr__(key, value)


# ------------------------------------------------------------------------------------------------ #
# mypy: allow-any-generics
# ------------------------------------------------------------------------------------------------ #
IMMUTABLE_TYPES: Tuple = (
    str,
    int,
    float,
    bool,
    type(None),
    np.int16,
    np.int32,
    np.int64,
    np.int8,
    np.float16,
    np.float32,
    np.float64,
    np.float128,
)
SEQUENCE_TYPES: Tuple = (
    list,
    tuple,
)
# ------------------------------------------------------------------------------------------------ #
NUMERICS = [
    "int16",
    "int32",
    "int64",
    "float16",
    "float32",
    "float64",
    np.int16,
    np.int32,
    np.int64,
    np.int8,
    np.float16,
    np.float32,
    np.float64,
    np.float128,
]


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DataClass(ABC):  # noqa
    """Base Class for Data Transfer Objects"""

    def __repr__(self) -> str:
        return "{}({})".format(
            self.__class__.__name__,
            ", ".join(
                "{}={!r}".format(k, v)
                for k, v in self.__dict__.items()
                if type(v) in IMMUTABLE_TYPES
            ),
        )

    def __str__(self) -> str:
        width = 32
        breadth = width * 2
        s = f"\n\n{self.__class__.__name__.center(breadth, ' ')}"
        d = self.as_dict()
        for k, v in d.items():
            if type(v) in IMMUTABLE_TYPES:
                s += f"\n{k.rjust(width,' ')} | {v}"
        s += "\n\n"
        return s

    def as_dict(self) -> Dict[str, Union[str, int, float, datetime, None]]:
        """Returns a dictionary representation of the the Config object."""
        return {
            k: self._export_config(v)
            for k, v in self.__dict__.items()
            if not k.startswith("_")
        }

    @classmethod
    def _export_config(
        cls,
        v: Any,
    ) -> Any:
        """Returns v with Configs converted to dicts, recursively."""
        if isinstance(v, IMMUTABLE_TYPES):
            return v
        elif isinstance(v, SEQUENCE_TYPES):
            return type(v)(map(cls._export_config, v))
        elif isinstance(v, dict):
            return v
        elif hasattr(v, "as_dict"):
            return v.as_dict()
        elif isinstance(v, Enum):
            if hasattr(v, "description"):
                return v.description
            else:
                return v.value
        elif isinstance(v, datetime):
            return v.isoformat()
        else:
            return dict()

    def as_df(self) -> Any:
        """Returns the project in DataFrame format"""
        d = self.as_dict()
        return pd.DataFrame(data=d, index=[0])


# ------------------------------------------------------------------------------------------------ #
class DataFrameType(Enum):
    """
    Enumeration representing different types of DataFrames and their characteristics.

    Attributes:
        PANDAS (tuple): Represents a Pandas DataFrame.
            - `distributed`: False
            - `nlp`: False
        SPARK (tuple): Represents a Spark DataFrame.
            - `distributed`: True
            - `nlp`: False
        SPARKNLP (tuple): Represents a Spark DataFrame with NLP capabilities.
            - `distributed`: True
            - `nlp`: True
    """

    PANDAS = ("pandas", False, False)
    SPARK = ("spark", True, False)
    SPARKNLP = ("sparknlp", True, True)

    def __new__(cls, value: str, distributed: bool, nlp: bool) -> DataFrameType:
        obj = object.__new__(cls)
        obj._value_ = value  # Set the Enum value
        obj._distributed = distributed
        obj._nlp = nlp
        return obj

    @property
    def identifier(self) -> str:
        """Returns the string identifier of the DataFrame type."""
        return self._value_

    @property
    def distributed(self) -> bool:
        """Indicates if the DataFrame type supports distributed computing."""
        return self._distributed

    @property
    def nlp(self) -> bool:
        """Indicates if the DataFrame type supports NLP-specific functionality."""
        return self._nlp

    @classmethod
    def from_identifier(cls, identifier: str) -> DataFrameType:
        """
        Finds the enum member based on its string identifier.

        Args:
            identifier (str): The string identifier of the DataFrame type (e.g., "pandas").

        Returns:
            DataFrameType: The matching enum member.

        Raises:
            ValueError: If no matching enum member is found.
        """
        for member in cls:
            if member._value_ == identifier:  # Compare against the _value_ attribute
                return member
        raise ValueError(f"No matching {cls.__name__} for identifier: {identifier}")
