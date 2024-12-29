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
# Modified   : Sunday December 29th 2024 12:49:06 pm                                               #
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
from discover.flow import SparkSessionType
from discover.infra.utils.file.info import FileStats


# ------------------------------------------------------------------------------------------------ #
#                                   DATA COMPONENT                                                 #
# ------------------------------------------------------------------------------------------------ #
@dataclass(config=dict(arbitrary_types_allowed=True))
class DataComponent(DatasetComponent):
    dftype: DFType
    filepath: str
    file_format: FileFormat
    created: Optional[datetime] = None
    # Private item created to ensure pydantic skips validation of this member.
    _data: Union[pd.DataFrame, DataFrame] = Field(exclude=True)
    spark_session_type: Optional[SparkSessionType] = SparkSessionType.SPARK

    def __post_init__(self) -> None:
        self.created = self.created if self.created else datetime.now()

    @property
    def size(self) -> int:
        return FileStats.get_size(path=self.filepath, in_bytes=True)

    @property
    def data(self) -> Optional[Union[pd.DataFrame, DataFrame]]:
        return self._data

    @data.setter
    def data(self, data: Union[pd.DataFrame, DataFrame]) -> None:
        if isinstance(data, (pd.DataFrame, DataFrame)):
            self._data = data
        else:
            raise TypeError(
                f"Data of type: {type(data)} is invalid. Expected a spark or pandas DataFrame"
            )

    @property
    def accessed(self) -> str:
        """The last accessed timestamp of the dataset file."""
        return FileStats.file_last_accessed(filepath=self.filepath)
