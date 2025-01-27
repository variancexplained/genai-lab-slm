#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/core/dstruct.py                                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday August 26th 2024 10:17:42 pm                                                 #
# Modified   : Sunday January 26th 2025 10:38:16 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data Structures Module"""
from __future__ import annotations

from abc import ABC
from datetime import datetime
from enum import Enum
from types import SimpleNamespace
from typing import Any, Dict, Mapping, Union

import pandas as pd
from genailab.core.dtypes import IMMUTABLE_TYPES, SEQUENCE_TYPES


# ------------------------------------------------------------------------------------------------ #
#                                 NESTED NAMESPACE                                                 #
# ------------------------------------------------------------------------------------------------ #
class NestedNamespace(SimpleNamespace):
    """
    A recursive namespace object for handling nested dictionaries.
    Automatically converts nested dictionaries into NestedNamespace instances.
    Supports pickling and unpickling.
    """

    def __init__(self, dictionary: Mapping[str, Any]) -> None:
        """
        Initializes the NestedNamespace.

        Args:
            dictionary (Mapping[str, Any]): The dictionary to convert into a namespace.
                Nested dictionaries are recursively converted.
        """
        super().__init__()
        for key, value in dictionary.items():
            # Recursively convert nested dictionaries to NestedNamespace
            if isinstance(value, dict):
                self.__setattr__(key, NestedNamespace(value))
            else:
                self.__setattr__(key, value)

    def __reduce__(self):
        """
        Customize pickling behavior to handle nested namespaces properly.

        Returns:
            tuple: A tuple describing how to pickle and unpickle the object.
        """
        return (self.__class__, (self.to_dict(),))

    def to_dict(self) -> Dict:
        """
        Converts the NestedNamespace back into a dictionary.

        Returns:
            dict: The dictionary representation of the NestedNamespace.
        """
        result = {}
        for key, value in self.__dict__.items():
            if isinstance(value, NestedNamespace):
                result[key] = value.to_dict()
            else:
                result[key] = value
        return result


# ------------------------------------------------------------------------------------------------ #
#                                    DATACLASS                                                     #
# ------------------------------------------------------------------------------------------------ #
class DataClass(ABC):
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
                k = k.strip("_")
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
        elif isinstance(v, datetime):
            return v.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(v, Enum) and hasattr(v, "description"):
            return v.description
        elif isinstance(v, Enum) and hasattr(v, "label"):
            return v.label
        elif isinstance(v, Enum) and hasattr(v, "value"):
            return v.value
        elif hasattr(v, "as_dict"):
            return v.as_dict()
        else:
            return dict()

    def as_df(self) -> Any:
        """Returns the project in DataFrame format"""
        d = self.as_dict()
        return pd.DataFrame(data=d, index=[0])
