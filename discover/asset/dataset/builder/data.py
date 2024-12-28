#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/asset/dataset/builder/data.py                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday December 27th 2024 10:20:36 pm                                               #
# Modified   : Saturday December 28th 2024 02:43:59 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Data Component Builder Module"""
from __future__ import annotations

from datetime import datetime
from typing import Union

import pandas as pd
from pyspark.sql import DataFrame

from discover.asset.dataset.base import DatasetComponentBuilder
from discover.asset.dataset.component.data import (
    DataEnvelope,
    DataFrameFileConfig,
    DFType,
    FileFormat,
)
from discover.asset.dataset.component.identity import DatasetPassport
from discover.infra.workspace.service import Workspace


# ------------------------------------------------------------------------------------------------ #
#                                DATA SOURCE BUILDER                                               #
# ------------------------------------------------------------------------------------------------ #
class DataSourceBuilder(DatasetComponentBuilder):

    def __init__(self, dataset_builder):
        self._dataset_builder = dataset_builder
        self._data = None
        self._dtype = None

        self._workspace = None
        self._passport = None
        self._envelope = None

    @property
    def envelope(self) -> DataEnvelope:
        envelope = self._envelope
        self.reset()
        return envelope

    def reset(self) -> None:
        self._data = None
        self._filepath = None
        self._dftype = None
        self._file_format = None
        self._created = None

        self._workspace = None
        self._passport = None
        self._envelope = None

    def data(self, data: Union[pd.DataFrame, DataFrame]) -> DataEnvelopeBuilder:
        self._data = data
        return self

    def filepath(self, filepath: str) -> DataEnvelopeBuilder:
        self._filepath = filepath
        return self

    def pandas(self) -> DataEnvelopeBuilder:
        self._dftype = DFType.PANDAS
        return self

    def spark(self) -> DataEnvelopeBuilder:
        self._dftype = DFType.SPARK
        return self

    def sparknlp(self) -> DataEnvelopeBuilder:
        self._dftype = DFType.SPARKNLP
        return self

    def csv(self) -> DataEnvelopeBuilder:
        self._file_format = FileFormat.CSV
        return self

    def parquet(self) -> DataEnvelopeBuilder:
        self._file_format = FileFormat.PARQUET
        return self

    def build(self) -> DataFrameFileConfigBuilder:
        """
        Builds and returns the final Dataset object based on the provided configurations.

        Returns:
            Dataset: The fully constructed dataset.
        """
        self._validate()
        self._workspace = self._get_workspace()
        self._passport = self._get_passport()
        self._filepath = self._get_filepath()

        self._envelope = DataEnvelope(
            data=self._data,
            filepath=self._filepath,
            dftype=self._dftype,
            file_format=self._file_format,
            created=datetime.now(),
        )

        return self

    def _get_workspace(self) -> Workspace:
        return self._dataset_builder.workspace

    def _get_passport(self) -> DatasetPassport:
        return self._dataset_builder.passport_ref

    def _get_filepath(self) -> str:
        return self._workspace.get_filepath(
            asset_id=self._passport.asset_id, file_format=self._file_format
        )

    def _validate(self) -> None:
        msg = ""
        msg += (
            f"Data type is invalid. Expected a pandas, spark or sparknlp DataFrame. Received object of type {type(self._data)}.\n"
            if not isinstance(self._data, (pd.DataFrame, DataFrame))
            else ""
        )
        msg += (
            f"File format {self._file_format} is invalid. Expected csv or parquet.\n"
            if not isinstance(self._file_format, FileFormat)
            else ""
        )
        msg += (
            f"File path is invalid. Expected a path or string object, received type {type(self._filepath)}.\n"
            if not isinstance(self._filepath, str)
            else ""
        )
        msg += (
            f"Passport is invalid. Expected a DatasetPassport object. Received object of type {type(self._passport)}. The passport must be configured before the data envelope.\n"
            if not isinstance(self._passport, DatasetPassport)
            else ""
        )
        if msg:
            raise ValueError(msg)


# ------------------------------------------------------------------------------------------------ #
#                            DATAFRAME IO SPEC BUILDER                                             #
# ------------------------------------------------------------------------------------------------ #
class DataFrameFileConfigBuilder(DatasetComponentBuilder):

    def __init__(self, dataset_builder):
        self._dataset_builder = dataset_builder
        self._dftype = None
        self._file_format = None
        self._filepath = None
        self._df_io_spec = None

    @property
    def spec(self) -> DataFrameFileConfig:
        df_io_spec = self._df_io_spec
        self.reset()
        return df_io_spec

    def reset(self) -> None:
        self._dftype = None
        self._file_format = None
        self._filepath = None
        self._df_io_spec = None

    def pandas(self) -> DataFrameFileConfigBuilder:
        self._dftype = DFType.PANDAS
        return self

    def spark(self) -> DataFrameFileConfigBuilder:
        self._dftype = DFType.SPARK
        return self

    def sparknlp(self) -> DataFrameFileConfigBuilder:
        self._dftype = DFType.SPARKNLP
        return self

    def csv(self) -> DataFrameFileConfigBuilder:
        self._file_format = FileFormat.CSV
        return self

    def parquet(self) -> DataFrameFileConfigBuilder:
        self._file_format = FileFormat.PARQUET
        return self

    def build(self) -> DataFrameFileConfigBuilder:
        """
        Builds and returns the final Dataset object based on the provided configurations.

        Returns:
            Dataset: The fully constructed dataset.
        """
        self._validate()
        self._filepath = self._get_filepath()
        self._df_io_spec = DataFrameFileConfig(
            dftype=self._dftype, filepath=self._filepath, file_format=self._file_format
        )
        return self

    def _get_filepath(self) -> str:
        workspace = self._dataset_builder.workspace
        passport = self._dataset_builder.passport_ref
        return workspace.get_filepath(
            asset_id=passport.asset_id, file_format=self._file_format
        )

    def _validate(self) -> None:
        msg = ""
        msg += (
            f"DataFrame type {self._dftype} is invalid. Expected pandas, spark, or sparknlp type.\n"
            if not isinstance(self._dftype, DFType)
            else ""
        )
        msg += (
            f"File format {self._file_format} is invalid. Expected csv or parquet.\n"
            if not isinstance(self._file_format, FileFormat)
            else ""
        )
        if msg:
            raise ValueError(msg)
