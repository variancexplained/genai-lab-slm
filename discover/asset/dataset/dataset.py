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
# Modified   : Sunday December 29th 2024 05:56:51 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Core Module"""
from __future__ import annotations

from discover.asset.base.asset import Asset
from discover.asset.dataset.base import DatasetComponent
from discover.asset.dataset.component.data import DataComponent
from discover.asset.dataset.component.identity import DatasetPassport
from discover.infra.workspace.service import Workspace


# ------------------------------------------------------------------------------------------------ #
#                                        DATASET                                                   #
# ------------------------------------------------------------------------------------------------ #
class Dataset(Asset):

    def __init__(
        self,
        workspace: Workspace,
        passport: DatasetPassport,
        data: DataComponent,
    ) -> None:
        super().__init__(passport=DatasetPassport)
        self._workspace = workspace
        self._passport = passport
        self._data = data

        self._is_composite = False

    # --------------------------------------------------------------------------------------------- #
    #                                  DATASET PROPERTIES                                           #
    # --------------------------------------------------------------------------------------------- #
    @property
    def data(self) -> DatasetComponent:
        return self._data

    @property
    def passport(self) -> DatasetPassport:
        return self._passport

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
