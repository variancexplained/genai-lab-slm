#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/domain/base.py                                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday June 30th 2024 10:32:43 pm                                                   #
# Modified   : Monday July 1st 2024 04:34:19 am                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Entity Module"""
from __future__ import annotations

import json
import string
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Callable

import numpy as np
import pandas as pd

from appinsight.utils.data import IMMUTABLE_TYPES, SEQUENCE_TYPES


# ------------------------------------------------------------------------------------------------ #
class Entity(ABC):
    """Base class for all entities"""

    def __eq__(self, other: Entity) -> bool:
        for key, value in self.__dict__.items():
            if type(value) in IMMUTABLE_TYPES:
                if value != other.__dict__[key]:
                    return False
            elif isinstance(value, np.ndarray):
                if not np.array_equal(value, other.__dict__[key]):
                    return False
            elif isinstance(value, (pd.DataFrame, pd.Series)):
                if not self.__dict__[key].equals(other.__dict__[key]):
                    return False

        return True

    def __repr__(self) -> str:  # pragma: no cover tested, but missing in coverage
        s = "{}({})".format(
            self.__class__.__name__,
            ", ".join(
                "{}={!r}".format(k, v)
                for k, v in self.__dict__.items()
                if type(v) in IMMUTABLE_TYPES
            ),
        )
        return s

    def __str__(self) -> str:
        width = 50
        breadth = width * 2
        s = f"\n\n{self.__class__.__name__.center(breadth, ' ')}"
        d = self.as_dict()
        for k, v in d.items():
            if type(v) in IMMUTABLE_TYPES:
                k = string.capwords(
                    k.replace(
                        "_",
                        " ",
                    )
                )
                s += f"\n{k.rjust(width,' ')} | {v}"
        s += "\n\n"
        return s

    @property
    @abstractmethod
    def oid(self) -> int:
        """Returns the unique identifier for the entity."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Returns the name for the entity."""

    @property
    @abstractmethod
    def phase(self) -> str:
        """Returns the phase in which the entity was created."""

    @property
    @abstractmethod
    def stage(self) -> str:
        """Returns the stage in which the entity was created."""

    @abstractmethod
    def validate(self) -> None:
        """Validates the entity. Should be called when exporting data to be persisted"""

    def print(self, keys: list) -> str:
        width = 50
        breadth = width * 2
        s = f"\n\n{self.__class__.__name__.center(breadth, ' ')}"
        d1 = self.as_dict()
        d2 = {k: d1[k] for k in keys}
        for k, v in d2.items():
            if type(v) in IMMUTABLE_TYPES:
                k = string.capwords(
                    k.replace(
                        "_",
                        " ",
                    )
                )
                s += f"\n{k.rjust(width,' ')} | {v}"
        s += "\n\n"
        return s

    def as_dict(self) -> dict:
        """Returns a dictionary representation of the the FileManager object."""
        return {
            k: self._export(v)
            for k, v in self.__dict__.items()
            if not k.startswith("_")
        }

    @classmethod
    def _export(cls, v):  # pragma: no cover
        """Returns v with FileManagers converted to dicts, recursively."""
        if isinstance(v, IMMUTABLE_TYPES):
            return v
        elif isinstance(v, SEQUENCE_TYPES):
            return type(v)(map(cls._export, v))
        elif isinstance(v, datetime):
            return v
        elif isinstance(v, dict):
            return json.dumps(v)
        elif hasattr(v, "as_dict"):
            return v.as_dict()
        elif isinstance(v, Callable):
            return v.__name__
        elif isinstance(v, object):
            return v.__class__.__name__

    def as_df(self) -> pd.DataFrame:
        """Returns the project in DataFrame format"""
        d = self.as_dict()
        return pd.DataFrame(data=d, index=[0])


# ------------------------------------------------------------------------------------------------ #
