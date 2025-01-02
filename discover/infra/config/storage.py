#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/config/storage.py                                                   #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday October 11th 2024 09:42:37 pm                                                #
# Modified   : Thursday January 2nd 2025 06:46:19 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from typing import Any, Dict

from pydantic.dataclasses import dataclass, field

from discover.core.dstruct import DataClass


# ------------------------------------------------------------------------------------------------ #
@dataclass(config=dict(arbitrary_types_allowed=True))
class StorageConfig(DataClass):
    """
    Base class that defines the interface for dataset content storage medium specific configurations.

    This class encapsulates configuration parameters for reading from and writing to a storage medium.
    It provides dictionaries to store any additional keyword arguments required for read and write
    operations, allowing for flexible storage configurations.

    Attributes:
        read_kwargs (Dict[str, Any]): A dictionary containing additional parameters for read operations.
            These parameters can be used to customize how data is read from the storage medium.
            Defaults to an empty dictionary.

        write_kwargs (Dict[str, Any]): A dictionary containing additional parameters for write operations.
            These parameters can be used to customize how data is written to the storage medium.
            Defaults to an empty dictionary.

    Example:
        >>> config = StorageConfig(
        ...     read_kwargs={"encoding": "utf-8", "compression": "gzip"},
        ...     write_kwargs={"compression": "gzip", "mode": "overwrite"}
        ... )
        >>> print(config.read_kwargs)
        {'encoding': 'utf-8', 'compression': 'gzip'}
        >>> print(config.write_kwargs)
        {'compression': 'gzip', 'mode': 'overwrite'}
    """

    read_kwargs: Dict[str, Any] = field(
        default_factory=dict
    )  # Additional read arguments
    write_kwargs: Dict[str, Any] = field(
        default_factory=dict
    )  # Additional write arguments
