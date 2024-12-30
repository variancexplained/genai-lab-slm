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
# Modified   : Monday December 30th 2024 03:38:07 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Data Module"""
from __future__ import annotations

from datetime import datetime
from typing import Optional, Union

import pandas as pd
from pydantic.dataclasses import dataclass
from pyspark.sql import DataFrame

from discover.asset.base.asset import Asset
from discover.asset.dataset.base import DatasetComponent
from discover.infra.utils.file.info import FileMeta


# ------------------------------------------------------------------------------------------------ #
#                                   DATA COMPONENT                                                 #
# ------------------------------------------------------------------------------------------------ #
@dataclass(config=dict(arbitrary_types_allowed=True))
class DataComponent(DatasetComponent):
    file_meta: FileMeta
    data: Optional[Union[pd.DataFrame, DataFrame]] = None

    def __post_init__(self) -> None:
        self.created = self.created if self.created else datetime.now()

    def __eq__(self, other: object) -> bool:
        """Checks equality between two Asset objects based on their asset ID."""
        if not isinstance(other, Asset):
            return NotImplemented
        return self.file_meta == other.file_meta

    def as_df(self) -> Optional[Union[pd.DataFrame, DataFrame]]:
        return self._data
