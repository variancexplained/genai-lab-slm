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
# Modified   : Sunday January 19th 2025 02:24:04 pm                                                #
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
from discover.analytics.summary import DatasetSummarizer
from discover.asset.base.asset import Asset
from discover.asset.dataset.identity import DatasetPassport
from discover.core.file import FileFormat
from discover.infra.utils.data.info import DataFrameInfo
from discover.infra.utils.file.info import FileMeta
from discover.infra.workspace.service import Workspace

# ------------------------------------------------------------------------------------------------ #
#                                   DATA COMPONENT                                                 #
# ------------------------------------------------------------------------------------------------ #


class Dataset(Asset):

    def __init__(
        self,
        workspace: Workspace,
        passport: DatasetPassport,
        filepath: str,
        file_format: FileFormat,
        dataframe: Union[pd.DataFrame, DataFrame],
        df_info_cls: Type[DataFrameInfo] = DataFrameInfo,
        file_meta_cls: Type[FileMeta] = FileMeta,
        summarizer_cls: Type[DatasetSummarizer] = DatasetSummarizer,
    ) -> None:
        super().__init__(passport=passport)
        self._workspace = workspace
        self._filepath = filepath
        self._file_format = file_format
        self._dataframe = dataframe

        self._file_meta = None
        self._info = None
        self._dqa = None
        self._summary = None

        self._df_info = df_info_cls()
        self._file_meta_cls = file_meta_cls
        self._summarizer = summarizer_cls()

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def __eq__(self, other: object) -> bool:
        """Checks equality between two Asset objects based on their asset ID."""
        if isinstance(other, Dataset):
            if self._passport == other.passport and self.file.path == other.file.path:
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
    def passport(self) -> DatasetPassport:
        return self._passport

    @property
    def file(self) -> FileMeta:
        self._set_file_meta()
        return self._file_meta

    @property
    def info(self) -> pd.DataFrame:
        if not self._info:
            self._set_info()
        return self._info

    @property
    def summary(self) -> None:
        self._summarize()

    @property
    def dqa(self) -> DQA:
        if not self._dqa:
            msg = "Dataset has no DQA property. Ensure that the Dataset has gone through the Data Quality Assessment Stage. Once complete, pass the Dataset to the Data Quality Analysis class whereby the dqa property is assigned."
            self._logger.error(msg)
            return None
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

    def _set_file_meta(self) -> None:
        """Sets file metadata if not set or incomplete."""
        if self._file_meta is None or self._file_meta.size is None:
            self._file_meta = self._file_meta_cls.create(
                path=self._filepath, file_format=self._file_format
            )

    def _set_info(self) -> None:
        """Sets the DataFrame info property"""

        if isinstance(self.dataframe, (pd.DataFrame, pd.core.frame.DataFrame)):
            self._info = self._df_info.pandas_info(df=self.dataframe)
        elif isinstance(self.dataframe, DataFrame):
            self._info = self._df_info.spark_info(df=self.dataframe)
        else:
            msg = f"The dataframe member of type {type(self.dataframe)} is not a valid pandas or spark DataFrame object."
            self._logger.error(msg)
            raise TypeError(msg)

    def _summarize(self) -> None:
        """Summarizes the Dataset"""

        if isinstance(self.dataframe, (pd.DataFrame, pd.core.frame.DataFrame)):
            self._summarizer.summarize_pandas(df=self.dataframe)
        elif isinstance(self.dataframe, DataFrame):
            self._summarizer.summarize_spark(df=self.dataframe)
        else:
            msg = f"The dataframe member of type {type(self.dataframe)} is not a valid pandas or spark DataFrame object."
            self._logger.error(msg)
            raise TypeError(msg)
