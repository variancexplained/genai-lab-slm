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
# Modified   : Tuesday December 31st 2024 05:39:58 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Data Module"""
from __future__ import annotations

import logging
from typing import Union

import pandas as pd
from pyspark.sql import DataFrame

from discover.asset.base.asset import Asset
from discover.asset.dataset import DFType, FileFormat
from discover.asset.dataset.base import DatasetComponent
from discover.asset.dataset.component.identity import DatasetPassport
from discover.infra.utils.file.info import FileInfo, FileMeta

# ------------------------------------------------------------------------------------------------ #
#                                   DATA COMPONENT                                                 #
# ------------------------------------------------------------------------------------------------ #


class DataComponent(DatasetComponent):

    def __init__(
        self,
        passport: DatasetPassport,
        filepath: str,
        data: Union[pd.DataFrame, DataFrame],
        file_format: FileFormat = FileFormat.PARQUET,
    ) -> None:
        self._passport = passport
        self._filepath = filepath
        self._file_format = file_format
        self._data = data
        self._file_meta = None

        self._dftype = None

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def __eq__(self, other: object) -> bool:
        """Checks equality between two Asset objects based on their asset ID."""
        if not isinstance(other, Asset):
            return NotImplemented
        return self.file_meta == other.file_meta

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
    def file_meta(self) -> FileMeta:
        if not self._file_meta:
            self._file_meta = self._get_file_meta()
        return self._file_meta

    @file_meta.setter
    def file_meta(self, file_meta) -> None:
        self._file_meta = file_meta

    @property
    def data(self) -> Union[pd.DataFrame, DataFrame]:
        return self._data

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
        if isinstance(self._data, (pd.DataFrame, pd.core.frame.DataFrame)):
            return DFType.PANDAS
        elif isinstance(self._data, DataFrame):
            return DFType.SPARK
        else:
            msg = f"Unrecognized data type. Expected a pandas or spark DataFrame. Received a {type(self._data)} object."
            self._logger.error(msg)
            raise TypeError(msg)
