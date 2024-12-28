#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/asset/dataset/dataset.py                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 01:35:04 am                                              #
# Modified   : Saturday December 28th 2024 03:44:12 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Core Module"""
from __future__ import annotations

from typing import Optional

from discover.asset.base import Asset
from discover.asset.dataset.base import DatasetComponent
from discover.asset.dataset.component.data import DataComponent
from discover.asset.dataset.component.identity import DatasetPassport
from discover.asset.dataset.component.ops import DatasetOps


# ------------------------------------------------------------------------------------------------ #
#                                        DATASET                                                   #
# ------------------------------------------------------------------------------------------------ #
class Dataset(Asset):

    def __init__(
        self,
        passport: DatasetPassport,
        data: DataComponent,
        ops: Optional[DatasetOps] = None,
    ) -> None:
        super().__init__(passport=DatasetPassport)

        self._data = data
        self._ops = ops

        self._is_composite = False

    # --------------------------------------------------------------------------------------------- #
    #                                  DATASET PROPERTIES                                           #
    # --------------------------------------------------------------------------------------------- #
    @property
    def data(self) -> DatasetComponent:
        return self._data

    @property
    def ops(self) -> DatasetOps:
        return self._ops

    # --------------------------------------------------------------------------------------------- #
    #                                      SERIALIZATION                                            #
    # --------------------------------------------------------------------------------------------- #
    def __getstate__(self) -> dict:
        """Prepares the object's state for serialization.

        This method converts the object's attributes into a dictionary
        that can be serialized, ensuring compatibility with serialization
        libraries and allowing the asset's state to be stored or transmitted.

        Returns:
            dict: A dictionary representation of the object's state.
        """
        state = self.__dict__.copy()
        state["_data"] = None  # Exclude data from serialization
        return state

    def __setstate__(self, state) -> None:
        """Restores the object's state during deserialization.

        Args:
            state (dict): The state dictionary to restore.
        """
        self.__dict__.update(state)
