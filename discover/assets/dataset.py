#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/assets/dataset.py                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 01:35:04 am                                              #
# Modified   : Monday December 16th 2024 03:06:46 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Module"""
from __future__ import annotations

from datetime import datetime
from typing import Optional, Union

import pandas as pd
from pyspark.sql import DataFrame

from discover.assets.base import Asset, AssetMeta
from discover.core.flow import PhaseDef, StageDef


# ------------------------------------------------------------------------------------------------ #
class DatasetMeta(AssetMeta):

    __asset_type: str = "dataset"

    def __init__(
        self,
        phase: PhaseDef,
        stage: StageDef,
        name: str,
    ):
        super().__init__(phase=phase, stage=stage, name=name)
        self._consumed: bool = False
        self._consumed_by: Optional[StageDef] = None
        self._dt_consumed: Optional[datetime] = None
        self._location = None
        self._approved = False

    @property
    def asset_type(self) -> str:
        return self.__asset_type

    @property
    def approved(self) -> bool:
        return self._approved

    @property
    def consumed(self) -> bool:
        return self._consumed

    def approve(self) -> None:
        self._approved = True

    def consume(self, asset_id: str) -> None:
        self._consumed = True
        self._consumed_by = asset_id
        self._dt_consumed = datetime.now()

    @classmethod
    def deserialize(cls, state: dict) -> DatasetMeta:
        # Create and return the DatasetMeta instance
        instance = cls.__new__(cls)  # Create a new instance without calling __init__
        instance.__setstate__(state)  # Set the state attributes manually
        return instance


# ------------------------------------------------------------------------------------------------ #
class Dataset(Asset):

    def __init__(
        self, meta: AssetMeta, content: Union[pd.DataFrame, DataFrame]
    ) -> None:
        super.__init__(meta=meta, content=content)

    def approve(self) -> None:
        self._meta.approve()
