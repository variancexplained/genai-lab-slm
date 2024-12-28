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
# Modified   : Saturday December 28th 2024 01:18:20 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Data Module"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Union

import pandas as pd
from pydantic.dataclasses import dataclass as pydantic_dataclass
from pyspark.sql import DataFrame

from discover.asset.dataset import DFType, FileFormat
from discover.asset.dataset.base import DatasetComponent
from discover.infra.utils.file.stats import FileStats


# ------------------------------------------------------------------------------------------------ #
#                                    DATA SOURCE                                                   #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DataSource(DatasetComponent):
    data: Union[pd.DataFrame, DataFrame]
    dftype: DFType


# ------------------------------------------------------------------------------------------------ #
#                                    FILE SOURCE                                                   #
# ------------------------------------------------------------------------------------------------ #
@pydantic_dataclass
class FileSource(DatasetComponent):
    filepath: str
    file_format: FileFormat


# ------------------------------------------------------------------------------------------------ #
#                                   DATA COMPONENT                                                 #
# ------------------------------------------------------------------------------------------------ #
@dataclass()
class DataComponent(DatasetComponent):

    dftype: DFType
    filepath: str
    file_format: FileFormat
    created: datetime
    data: Optional[Union[pd.DataFrame, DataFrame]] = None
    size: Optional[int] = None

    def __post_init__(self) -> None:
        if self._data:
            if (
                isinstance(self.data, pd.DataFrame) and self.dftype != DFType.PANDAS
            ) or (
                isinstance(self.data, DataFrame)
                and self.dftype not in (DFType.SPARK, DFType.SPARKNLP)
            ):
                msg = f"Data integrity error. The data type and dftype arguments are incompatible.\ndata type: {type(self.data)}\nDFType (dftype): {self.dftype.value}."
                raise ValueError(msg)

        # Compute size of the dataset
        if not self.size:
            self.size = FileStats.get_size(path=self.filepath, in_bytes=True)

    @property
    def accessed(self) -> str:
        """The last accessed timestamp of the dataset file."""
        return FileStats.file_last_accessed(filepath=self.filepath)
