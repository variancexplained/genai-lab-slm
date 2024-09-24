#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/core/data_class.py                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday August 26th 2024 10:17:42 pm                                                 #
# Modified   : Tuesday September 24th 2024 02:00:29 pm                                             #
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
from typing import Any, Dict, Tuple, Union

import numpy as np
import pandas as pd

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
    ) -> Any:  # pragma: no cover
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
