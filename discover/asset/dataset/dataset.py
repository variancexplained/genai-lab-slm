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
# Modified   : Tuesday December 31st 2024 09:57:51 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Core Module"""
from __future__ import annotations

from discover.analytics.dqa import DQA
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

        self._dqa = None

        self._is_composite = False

    def __eq__(self, other) -> bool:
        if (
            self.passport == other.passport
            and self.data.filepath == other.data.filepath
            and self.data.dftype == other.data.dftype
        ):
            return True
        else:
            return False

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
        return self.__dict__.copy()

    def __setstate__(self, state) -> None:
        """Restores the object's state during deserialization.

        Args:
            state (dict): The state dictionary to restore.
        """
        self.__dict__.update(state)

    # --------------------------------------------------------------------------------------------- #
    #                                  DATASET PROPERTIES                                           #
    # --------------------------------------------------------------------------------------------- #
    @property
    def asset_id(self) -> str:
        return self._passport.asset_id

    @property
    def data(self) -> DatasetComponent:
        return self._data

    @property
    def passport(self) -> DatasetPassport:
        return self._passport

    @property
    def dqa(self) -> DQA:
        if self._dqa is None:
            print("This Dataset has no DQA component.")
        else:
            return self._dqa

    @dqa.setter
    def dqa(self, dqa: DQA) -> None:
        self._dqa = dqa
