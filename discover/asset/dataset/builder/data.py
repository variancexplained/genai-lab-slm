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
# Modified   : Friday December 27th 2024 10:52:29 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Data Component Builder Module"""
from __future__ import annotations

from discover.asset.dataset.base import DatasetComponentBuilder
from discover.asset.dataset.component.data import DataFrameIOSpec, DFType, FileFormat


# ------------------------------------------------------------------------------------------------ #
#                            DATAFRAME IO SPEC BUILDER                                             #
# ------------------------------------------------------------------------------------------------ #
class DataFrameIOSpecBuilder(DatasetComponentBuilder):
    """
    Abstract base class for building assets with phases, stages, and persistence
    configurations.
    """

    def __init__(self, dataset_builder):
        self._dataset_builder = dataset_builder
        self._dftype = None
        self._file_format = None
        self._filepath = None
        self._df_io_spec

    @property
    def df_io_spec(self) -> DataFrameIOSpec:
        df_io_spec = self._df_io_spec
        self.reset()
        return df_io_spec

    def reset(self) -> None:
        """
        Resets the builder to be ready to construct another Dataset object.
        """
        self._dftype = None
        self._file_format = None
        self._filepath = None

    def pandas(self) -> DataFrameIOSpecBuilder:
        self._dftype = DFType.PANDAS
        return self

    def spark(self) -> DataFrameIOSpecBuilder:
        self._dftype = DFType.SPARK
        return self

    def sparknlp(self) -> DataFrameIOSpecBuilder:
        self._dftype = DFType.SPARKNLP
        return self

    def csv(self) -> DataFrameIOSpecBuilder:
        self._file_format = FileFormat.CSV
        return self

    def parquet(self) -> DataFrameIOSpecBuilder:
        self._file_format = FileFormat.PARQUET
        return self

    def build(self) -> DataFrameIOSpecBuilder:
        """
        Builds and returns the final Dataset object based on the provided configurations.

        Returns:
            Dataset: The fully constructed dataset.
        """
        self._filepath = self._get_filepath()
        self._df_io_spec = DataFrameIOSpec(
            dftype=self._dftype, filepath=self._filepath, file_format=self._file_format
        )
        return self

    def _get_filepath(self) -> str:
        workspace = self._dataset_builder.workspace
        passport = self._dataset_builder.passport_ref
        return workspace.get_filepath(
            asset_id=passport.asset_id, file_format=self._file_format
        )
