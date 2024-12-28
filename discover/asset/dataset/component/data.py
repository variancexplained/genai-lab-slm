#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/asset/dataset/component/data.py                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday December 27th 2024 08:32:52 pm                                               #
# Modified   : Saturday December 28th 2024 06:14:17 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Data Module"""
from __future__ import annotations

from datetime import datetime
from typing import Optional, Union

import pandas as pd
from pydantic import Field
from pydantic.dataclasses import dataclass
from pyspark.sql import DataFrame

from discover.asset.dataset import DFType, FileFormat
from discover.asset.dataset.base import DatasetComponent
from discover.infra.utils.file.stats import FileStats


# ------------------------------------------------------------------------------------------------ #
#                                    DATA SOURCE                                                   #
# ------------------------------------------------------------------------------------------------ #
@dataclass(config=dict(arbitrary_types_allowed=True))
class DataSource(DatasetComponent):
    dftype: DFType
    data: Union[pd.DataFrame, DataFrame] = Field(default=None, exclude=True)


# ------------------------------------------------------------------------------------------------ #
#                                    FILE SOURCE                                                   #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class FileSource(DatasetComponent):
    filepath: str
    file_format: FileFormat


# ------------------------------------------------------------------------------------------------ #
#                                   DATA COMPONENT                                                 #
# ------------------------------------------------------------------------------------------------ #
@dataclass(config=dict(arbitrary_types_allowed=True))
class DataComponent(DatasetComponent):

    dftype: DFType
    filepath: str
    file_format: FileFormat
    created: datetime
    # Private item created to ensure pydantic skips validation of this member.
    _data: Optional[Union[pd.DataFrame, DataFrame]] = Field(exclude=True)

    @property
    def size(self) -> int:
        return FileStats.get_size(path=self.filepath, in_bytes=True)

    @property
    def data(self) -> Optional[Union[pd.DataFrame, DataFrame]]:
        return self._data

    @data.setter
    def data(self, data: Union[pd.DataFrame, DataFrame]) -> None:
        self._data = data

    @property
    def accessed(self) -> str:
        """The last accessed timestamp of the dataset file."""
        return FileStats.file_last_accessed(filepath=self.filepath)
