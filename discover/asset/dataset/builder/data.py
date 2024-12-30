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
# Modified   : Monday December 30th 2024 03:42:06 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Data Component Builder Module"""
from __future__ import annotations

import logging
from typing import Union

import pandas as pd
from dependency_injector.wiring import Provide, inject
from pyspark.sql import DataFrame

from discover.asset.dataset import DFType, FileFormat
from discover.asset.dataset.base import DatasetComponentBuilder
from discover.asset.dataset.component.data import DataComponent
from discover.asset.dataset.component.identity import DatasetPassport
from discover.container import DiscoverContainer
from discover.infra.utils.file.info import ParquetFileDetector
from discover.infra.workspace.service import Workspace

# ------------------------------------------------------------------------------------------------ #
parquet_file_detector = ParquetFileDetector()


# ------------------------------------------------------------------------------------------------ #
#                                DATA COMPONENT BUILDER                                            #
# ------------------------------------------------------------------------------------------------ #
class DataComponentBuilder(DatasetComponentBuilder):

    @inject
    def __init__(
        self,
        workspace: Workspace = Provide[DiscoverContainer.workspace.service],
    ) -> None:
        self._workspace = workspace
        self._passport = None
        self._data = None
        self._data_component = None
        self._dftype = None
        self._file_format = None
        self._filepath = None

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def reset(self) -> None:
        """
        Resets the builder's internal state.

        Clears all attributes, preparing the builder for a new configuration.
        """
        self._passport = None
        self._data = None
        self._data_component = None
        self._dftype = None
        self._file_format = None
        self._filepath = None

    # -------------------------------------------------------------------------------------------- #
    @property
    def data(self) -> DataComponent:
        """
        Retrieves the constructed `Dataset` object and resets the builder's state.

        Returns:
            Dataset: The constructed dataset object.
        """
        data_component = self._data_component
        self.reset()
        return data_component

    # -------------------------------------------------------------------------------------------- #
    def passport(self, passport: DatasetPassport) -> DataComponentBuilder:
        """
        Sets the data for the `DataComponent`.

        Args:
            data (Union[pd.DataFrame, DataFrame]): The data to be included in the `DataComponent`.

        Returns:
            DataComponentBuilder: The current builder instance for chaining.
        """
        self._passport = passport
        return self

    # -------------------------------------------------------------------------------------------- #
    def source(self, data: Union[pd.DataFrame, DataFrame]) -> DataComponentBuilder:
        """
        Sets the data for the `DataComponent`.

        Args:
            data (Union[pd.DataFrame, DataFrame]): The data to be included in the `DataComponent`.

        Returns:
            DataComponentBuilder: The current builder instance for chaining.
        """
        self._data = data
        return self

    # -------------------------------------------------------------------------------------------- #
    #                                  DATAFRAME TYPES                                             #
    # -------------------------------------------------------------------------------------------- #
    def as_pandas(self) -> DataComponentBuilder:
        """
        Specifies that the data type is a Pandas DataFrame.

        Returns:
            DataComponentBuilder: The current builder instance for chaining.
        """
        self._dftype = DFType.PANDAS
        return self

    def as_spark(self) -> DataComponentBuilder:
        """
        Specifies that the data type is a Spark DataFrame.

        Returns:
            DataComponentBuilder: The current builder instance for chaining.
        """
        self._dftype = DFType.SPARK
        return self

    # -------------------------------------------------------------------------------------------- #
    #                                    FILE FORMATS                                              #
    # -------------------------------------------------------------------------------------------- #
    def to_csv(self) -> DataComponentBuilder:
        """
        Sets the file format to CSV and determines the file path.

        Returns:
            DataComponentBuilder: The current builder instance for chaining.
        """
        self._file_format = FileFormat.CSV
        return self

    def to_parquet(self) -> DataComponentBuilder:
        """
        Sets the file format to Parquet and determines the file path.

        Returns:
            DataComponentBuilder: The current builder instance for chaining.
        """
        self._file_format = FileFormat.PARQUET
        return self

    # -------------------------------------------------------------------------------------------- #
    #                                       BUILD                                                  #
    # -------------------------------------------------------------------------------------------- #
    def build(self) -> DataComponentBuilder:
        self._validate()
        self._data_component = DataComponent(
            dftype=self._dftype,
            filepath=self._filepath,
            file_format=self._file_format,
            _data=self._data,
        )
        return self

    def _get_filepath(self) -> str:
        """
        Generates the file path for the data component using the workspace and passport.

        Returns:
            str: The file path for the data component.
        """
        return self._workspace.get_filepath(
            asset_type=self._passport.asset_type,
            asset_id=self._passport.asset_id,
            phase=self._passport.phase,
            file_format=self._file_format,
        )

    def _validate(self) -> None:
        # Validate DataFrame type
        if not isinstance(
            self._data, (pd.DataFrame, pd.core.frame.DataFrame, DataFrame)
        ):
            msg = f"TypeError: Invalid data type for DataFrame. Expected DFType.PANDAS, DFType.SPARK, or DFType.SPARKNLP. Received type {type(self._data)}"
            self._logger.error(msg)
            raise TypeError(msg)

        # Validate / Infer DataFrame type if None
        if self._dftype is None:
            if isinstance(self._data, DataFrame):
                self._dftype = DFType.SPARK
            else:
                self._dftype = DFType.PANDAS
        else:
            if isinstance(self._data, DataFrame) and self._dftype == DFType.PANDAS:
                msg = f"DataIntegrityError: DataFrame type `dftype` {self._dftype.value} is incompatible with the data of type {type(self._data)}"
                self._logger.error(msg)
                raise ValueError(msg)
            if (
                isinstance(self._data, (pd.DataFrame, pd.core.frame.DataFrame))
                and self._dftype != DFType.PANDAS
            ):
                msg = f"DataIntegrityError: DataFrame type `dftype` {self._dftype.value} is incompatible with the data of type {type(self._data)}"
                self._logger.error(msg)
                raise ValueError(msg)
