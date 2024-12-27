#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/persist/file/base.py                                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday December 26th 2024 02:37:54 pm                                             #
# Modified   : Thursday December 26th 2024 08:43:47 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

# ------------------------------------------------------------------------------------------------ #
#                                       IO FACTORY                                                 #
# ------------------------------------------------------------------------------------------------ #


class IOFactory(ABC):
    """Defines the interface for factories that return objects that perform IO.

    This encapsulates IO for a range of data structures, such as DataFrames, arrays,
    and dictionaries.

    """

    @classmethod
    @abstractmethod
    def get_reader(cls, *args, **kwargs) -> Reader:
        """Returns a reader based on args, and kwargs defined in subclasses."""

    @classmethod
    @abstractmethod
    def get_writer(cls, *args, **kwargs) -> Writer:
        """Returns a writer based on args, and kwargs defined in subclasses."""


# ------------------------------------------------------------------------------------------------ #
#                                       READER                                                     #
# ------------------------------------------------------------------------------------------------ #
class Reader(ABC):

    @classmethod
    @abstractmethod
    def read(cls, filepath: str, **kwargs) -> Any:
        """Reads data from a file.

        Args:
            filepath (str): Path to the file to be read.
            **kwargs (dict): Keyword arguments passed to the underlying read mechanism.
        """


# ------------------------------------------------------------------------------------------------ #
#                                       WRITER                                                     #
# ------------------------------------------------------------------------------------------------ #
class Writer(ABC):

    @classmethod
    @abstractmethod
    def write(cls, filepath: str, data: Any, **kwargs) -> Any:
        """Writes data to a file

        Args:
            filepath (str): Path to the file to be read.
            data (Any): DAta to  be written to file.
            **kwargs (dict): Keyword arguments passed to the underlying write mechanism.
        """
