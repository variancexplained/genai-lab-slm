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
# Created    : Friday December 27th 2024 08:32:52 pm                                               #
# Modified   : Friday January 3rd 2025 03:14:47 am                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Data Module"""
from __future__ import annotations

import logging
from typing import Type, Union

import pandas as pd
from pyspark.sql import DataFrame

from discover.analytics.dqa import DQA
from discover.asset.base.asset import Asset
from discover.asset.dataset.identity import DatasetPassport
from discover.core.dtypes import DFType
from discover.core.file import FileFormat
from discover.infra.utils.file.info import FileInfo, FileMeta
from discover.infra.workspace.service import Workspace

# ------------------------------------------------------------------------------------------------ #
#                                   DATA COMPONENT                                                 #
# ------------------------------------------------------------------------------------------------ #


class Dataset(Asset):

    def __init__(
        self,
        workspace: Workspace,
        passport: DatasetPassport,
        dftype: DFType,
        filepath: str,
        file_format: FileFormat,
        dataframe: Union[pd.DataFrame, DataFrame],
        file_info_cls: Type[FileInfo] = FileInfo,
    ) -> None:
        super().__init__(passport=DatasetPassport)
        self._passport = passport
        self._dftype = dftype
        self._filepath = filepath
        self._file_format = file_format
        self._dataframe = dataframe
        self._file_meta = None
        self._file_info = file_info_cls()

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def __eq__(self, other: object) -> bool:
        """Checks equality between two Asset objects based on their asset ID."""
        if isinstance(other, Dataset):
            if self._passport == other.passport and self.filepath == other.filepath:
                return True
            else:
                return False
        else:
            return False

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

    @property
    def asset_id(self) -> str:
        return self._passport.asset_id

    @property
    def name(self) -> str:
        return self._passport.name

    @property
    def description(self) -> str:
        return self._passport.description

    @property
    def version(self) -> str:
        return self._passport.version

    @property
    def source(self) -> DatasetPassport:
        return self._passport.source

    @property
    def parent(self) -> DatasetPassport:
        return self._passport.parent

    @property
    def created(self) -> DatasetPassport:
        return self._passport.created

    @property
    def passport(self) -> DatasetPassport:
        return self._passport

    @property
    def dftype(self) -> DFType:
        return self._dftype

    @property
    def file_format(self) -> FileFormat:
        return self._file_format

    @property
    def filepath(self) -> str:
        return self._filepath

    @property
    def size(self) -> int:
        if not self._file_meta:
            self._set_file_meta()
        return self._file_meta.size

    @property
    def file_meta(self) -> FileMeta:
        if not self._file_meta:
            self._set_file_meta()
        return self._file_meta

    @property
    def dqa(self) -> DQA:
        if self._dqa is None:
            print("This Dataset has no DQA component.")
        else:
            return self._dqa

    @dqa.setter
    def dqa(self, dqa: DQA) -> None:
        self._dqa = dqa

    @property
    def dataframe(self) -> Union[pd.DataFrame, DataFrame]:
        return self._dataframe

    def serialize(self) -> pd.DataFrame:
        """Prepare the object for serialization by nullifying the dataframe."""
        # Copy the dataframe.
        df = self._dataframe

        # Nullify the dataframe for serialization
        self._dataframe = None
        return df

    def deserialize(self, dataframe: Union[pd.DataFrame, DataFrame]) -> None:
        self._dataframe = dataframe

    def _set_file_meta(self) -> FileMeta:
        try:
            self._file_meta = self._file_info.get_file_meta(filepath=self._filepath)
        except FileNotFoundError:
            raise FileNotFoundError(
                "The file does not exist. Either the dataset containing the file has not been persisted."
            )
        except Exception as e:
            msg = f"An unexpected error occurred while obtaining file metadata for asset {self._passport.asset_id}.\nFilepath:  {self._filepath}."
            raise Exception(msg) from e
