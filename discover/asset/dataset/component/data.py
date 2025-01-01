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
# Modified   : Tuesday December 31st 2024 08:42:40 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Data Module"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, Union

import pandas as pd
from pyspark.sql import DataFrame

from discover.asset.base.asset import Asset
from discover.asset.dataset import DFType, FileFormat
from discover.asset.dataset.base import DatasetComponent
from discover.asset.dataset.component.identity import DatasetPassport
from discover.core.dtypes import IMMUTABLE_TYPES
from discover.infra.utils.file.info import FileInfo, FileMeta

# ------------------------------------------------------------------------------------------------ #
#                                   DATA COMPONENT                                                 #
# ------------------------------------------------------------------------------------------------ #


class DataComponent(DatasetComponent):

    def __init__(
        self,
        passport: DatasetPassport,
        filepath: str,
        dataframe: Union[pd.DataFrame, DataFrame],
        file_format: FileFormat = FileFormat.PARQUET,
    ) -> None:
        self._passport = passport
        self._filepath = filepath
        self._file_format = file_format
        self._dataframe = dataframe
        self._file_meta = None

        self._dftype = None

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def __eq__(self, other: object) -> bool:
        """Checks equality between two Asset objects based on their asset ID."""
        if not isinstance(other, Asset):
            return NotImplemented
        return self.file_meta == other.file_meta

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

    def __repr__(self) -> str:
        return "{}({})".format(
            self.__class__.__name__,
            ", ".join(
                "{}={!r}".format(k, v)
                for k, v in self.__dict__.items()
                if type(v) in IMMUTABLE_TYPES
            ),
        )

    def __str__(self) -> str:
        width = 32
        breadth = width * 2
        s = f"\n\n{self.__class__.__name__.center(breadth, ' ')}"
        d = self.as_dict()
        for k, v in d.items():
            if type(v) in IMMUTABLE_TYPES:
                k = k.strip("_")
                s += f"\n{k.rjust(width,' ')} | {v}"
        s += "\n\n"
        return s

    def as_dict(self) -> Dict[str, Union[str, int, float, datetime, None]]:
        """Returns a dictionary representation of the the Config object."""
        return {
            k: self._export_config(v)
            for k, v in self.__dict__.items()
            if not k.startswith("_")
        }

    @property
    def asset_id(self) -> str:
        return self._passport.asset_id

    @property
    def passport(self) -> DatasetPassport:
        return self._passport

    @property
    def dftype(self) -> DFType:
        if not self._dftype:
            self._dftype = self._determine_dftype()
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
            self._file_meta = self._get_file_meta()
        return self._file_meta.size

    @property
    def file_meta(self) -> FileMeta:
        if not self._file_meta:
            self._file_meta = self._get_file_meta()
        return self._file_meta

    @file_meta.setter
    def file_meta(self, file_meta) -> None:
        self._file_meta = file_meta

    @property
    def dataframe(self) -> Union[pd.DataFrame, DataFrame]:
        return self._dataframe

    def serialize(self) -> pd.DataFrame:
        """Prepare the object for serialization by nullifying the dataframe."""
        self._logger.debug("Before serialization of _dataframe.")

        # Copy the dataframe.
        df = self._dataframe

        # Nullify the dataframe for serialization
        self._dataframe = None
        if self._dataframe is None:
            self._logger.debug("Serialized: _dataframe set to None.")
        else:
            self._logger.debug("Serialized: _dataframe nullification FAILED.")

        return df

    def deserialize(self, dataframe: Union[pd.DataFrame, DataFrame]) -> None:
        self._logger.debug(f"Before deserialization the dataframe is {self._dataframe}")
        self._dataframe = dataframe
        self._logger.debug(
            f"After deserialization the dataframe is {self._dataframe != None}"
        )

    def _get_file_meta(self) -> FileMeta:
        try:
            return FileInfo().get_file_meta(filepath=self._filepath)
        except FileNotFoundError:
            raise FileNotFoundError(
                "The file does not exist. Either the dataset containing the file has not been persisted."
            )
        except Exception as e:
            msg = f"An unexpected error occurred while obtaining file metadata for asset {self._passport.asset_id}.\nFilepath:  {self._filepath}."
            raise Exception(msg) from e

    def _determine_dftype(self) -> DFType:
        if isinstance(self._dataframe, (pd.DataFrame, pd.core.frame.DataFrame)):
            return DFType.PANDAS
        elif isinstance(self._dataframe, DataFrame):
            return DFType.SPARK
        else:
            msg = f"Unrecognized data type. Expected a pandas or spark DataFrame. Received a {type(self._dataframe)} object."
            self._logger.error(msg)
            raise TypeError(msg)
