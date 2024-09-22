#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/element/dataset/store.py                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 01:35:16 am                                              #
# Modified   : Sunday September 22nd 2024 12:26:55 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #


from dataclasses import dataclass, field
from typing import Any, Dict

from discover.element.base.store import StorageConfig


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatasetStorageConfig(StorageConfig):
    is_spark: bool = False
    partitioned: bool = True
    file_ext: str = ""

    def __post_init__(self) -> None:
        if self.partitioned:
            self.file_ext = ""
        else:
            self.file_ext = ".parquet"


# ------------------------------------------------------------------------------------------------ #
@dataclass
class PandasDatasetStorageConfig(DatasetStorageConfig):
    read_kwargs: Dict[str, Any] = field(default_factory=dict)
    write_kwargs: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.partitioned:
            self.write_kwargs = {
                "engine": "pyarrow",
                "compression": "snappy",
                "index": False,
                "partition_cols": ["category"],
            }
        else:
            self.write_kwargs = {
                "engine": "pyarrow",
                "compression": "snappy",
                "index": False,
            }


# ------------------------------------------------------------------------------------------------ #
@dataclass
class SparkDatasetStorageConfig(DatasetStorageConfig):
    is_spark: bool = True
    read_kwargs: Dict[str, Any] = field(default_factory=dict)
    write_kwargs: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.partitioned:
            self.write_kwargs = {
                "partition_cols": ["category"],
            }
