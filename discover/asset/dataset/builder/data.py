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
# Modified   : Wednesday January 1st 2025 01:37:29 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Data Component Builder Module"""
from __future__ import annotations

import logging
import os
from typing import Type, Union

import pandas as pd
from dependency_injector.wiring import Provide, inject
from pyspark.sql import DataFrame, SparkSession

from discover.asset.dataset import DFType, FileFormat
from discover.asset.dataset.base import DatasetComponentBuilder
from discover.asset.dataset.component.data import DataComponent
from discover.asset.dataset.component.identity import DatasetPassport
from discover.container import DiscoverContainer
from discover.infra.persist.file.fao import FAO
from discover.infra.service.spark.pool import SparkSessionPool
from discover.infra.utils.file.info import FileTypeDetector, ParquetFileDetector
from discover.infra.workspace.service import Workspace

# ------------------------------------------------------------------------------------------------ #
parquet_file_detector = ParquetFileDetector()


# ------------------------------------------------------------------------------------------------ #
#                       DATA COMPONENT BUILDER FROM DATAFRAME                                      #
# ------------------------------------------------------------------------------------------------ #
class DataComponentBuilderFromDataFrame(DatasetComponentBuilder):

    @inject
    def __init__(
        self,
        workspace: Workspace = Provide[DiscoverContainer.workspace.service],
    ) -> None:
        self._workspace = workspace
        self._passport = None
        self._dataframe = None
        self._data_component = None
        self._file_format = FileFormat.PARQUET
        self._filepath = None
        self._file_meta = None

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def reset(self) -> None:
        """
        Resets the builder's internal state.

        Clears all attributes, preparing the builder for a new configuration.
        """
        self._passport = None
        self._dataframe = None
        self._data_component = None
        self._dftype = DFType.PANDAS
        self._file_format = FileFormat.PARQUET
        self._filepath = None

    # -------------------------------------------------------------------------------------------- #
    @property
    def data_component(self) -> DataComponent:
        """
        Retrieves the constructed `Dataset` object and resets the builder's state.

        Returns:
            Dataset: The constructed dataset object.
        """
        data_component = self._data_component
        self.reset()
        return data_component

    # -------------------------------------------------------------------------------------------- #
    def dataframe(
        self, dataframe: Union[pd.DataFrame, DataFrame]
    ) -> DataComponentBuilderFromDataFrame:
        """
        Sets the data for the `DataComponent`.

        Args:
            data (Union[pd.DataFrame, DataFrame]): The data to be included in the `DataComponent`.

        Returns:
            DataComponentBuilderFromDataFrame: The current builder instance for chaining.
        """
        self._dataframe = dataframe
        return self

    # -------------------------------------------------------------------------------------------- #
    def passport(self, passport: DatasetPassport) -> DataComponentBuilderFromDataFrame:
        """
        Sets the data for the `DataComponent`.

        Args:
            data (Union[pd.DataFrame, DataFrame]): The data to be included in the `DataComponent`.

        Returns:
            DataComponentBuilderFromDataFrame: The current builder instance for chaining.
        """
        self._passport = passport
        return self

    # -------------------------------------------------------------------------------------------- #
    #                                    FILE FORMATS                                              #
    # -------------------------------------------------------------------------------------------- #
    def to_csv(self) -> DataComponentBuilderFromDataFrame:
        """
        Sets the file format to CSV and determines the file path.

        Returns:
            DataComponentBuilderFromDataFrame: The current builder instance for chaining.
        """
        self._file_format = FileFormat.CSV
        return self

    def to_parquet(self) -> DataComponentBuilderFromDataFrame:
        """
        Sets the file format to Parquet and determines the file path.

        Returns:
            DataComponentBuilderFromDataFrame: The current builder instance for chaining.
        """
        self._file_format = FileFormat.PARQUET
        return self

    # -------------------------------------------------------------------------------------------- #
    #                                       BUILD                                                  #
    # -------------------------------------------------------------------------------------------- #
    def build(self) -> DataComponentBuilderFromDataFrame:
        self._validate()

        self._filepath = self._get_filepath()

        self._data_component = DataComponent(
            passport=self._passport,
            filepath=self._filepath,
            file_format=self._file_format,
            dataframe=self._dataframe,
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
        # Ensure a passport is provided
        if not isinstance(self._passport, DatasetPassport):
            msg = "A DataComponent requires a DatasetPassport object for the Dataset to which this component belongs."
            self._logger.error(msg)
            raise TypeError(msg)
        # Validate DataFrame type
        if not isinstance(
            self._dataframe, (pd.DataFrame, pd.core.frame.DataFrame, DataFrame)
        ):
            msg = f"TypeError: Invalid data type for DataFrame. Expected DFType.PANDAS, DFType.SPARK, or DFType.SPARKNLP. Received type {type(self._dataframe)}"
            self._logger.error(msg)
            raise TypeError(msg)


# ------------------------------------------------------------------------------------------------ #
#                            DATA COMPONENT BUILDER FROM FILE                                      #
# ------------------------------------------------------------------------------------------------ #
class DataComponentBuilderFromFile(DatasetComponentBuilder):

    @inject
    def __init__(
        self,
        workspace: Workspace = Provide[DiscoverContainer.workspace.service],
        spark_session_pool: SparkSessionPool = Provide[
            DiscoverContainer.spark.session_pool
        ],
        fao: FAO = Provide[DiscoverContainer.io.fao],
        ftd_cls: Type[FileTypeDetector] = FileTypeDetector,
    ) -> None:
        self._workspace = workspace
        self._spark_session_pool = spark_session_pool
        self._fao = fao
        self._ftd = ftd_cls()

        self._passport = None
        self._dataframe = None
        self._data_component = None
        self._file_format = None
        self._filepath = None
        self._file_meta = None
        self._source_filepath = None

        self._spark = None

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def reset(self) -> None:
        """
        Resets the builder's internal state.

        Clears all attributes, preparing the builder for a new configuration.
        """
        self._passport = None
        self._dataframe = None
        self._data_component = None
        self._dftype = None
        self._file_format = None
        self._filepath = None
        self._source_filepath = None

    # -------------------------------------------------------------------------------------------- #
    @property
    def data_component(self) -> DataComponent:
        """
        Retrieves the constructed `Dataset` object and resets the builder's state.

        Returns:
            Dataset: The constructed dataset object.
        """
        data_component = self._data_component
        self.reset()
        return data_component

    # -------------------------------------------------------------------------------------------- #
    #                                  SOURCE FILEPATH                                             #
    # -------------------------------------------------------------------------------------------- #
    def source_filepath(self, source_filepath: str) -> DataComponentBuilderFromFile:
        """
        Sets the data for the `DataComponent`.

        Args:
            source_filepath (str): Path to source file.

        Returns:
            DataComponentBuilderFromDataFrame: The current builder instance for chaining.
        """
        self._source_filepath = source_filepath
        return self

    # -------------------------------------------------------------------------------------------- #
    #                                       SPARK                                                  #
    # -------------------------------------------------------------------------------------------- #
    def spark(self, spark: SparkSession) -> DataComponentBuilderFromFile:
        """
        Sets the spark session required to read spark dataframes.

        Args:
            spark (SparkSession): The spark session

        Returns:
            DataComponentBuilderFromDataFrame: The current builder instance for chaining.
        """
        self._spark = spark
        return self

    # -------------------------------------------------------------------------------------------- #
    #                                      PASSPORT                                                #
    # -------------------------------------------------------------------------------------------- #
    def passport(self, passport: DatasetPassport) -> DataComponentBuilderFromFile:
        """
        Sets the data for the `DataComponent`.

        Args:
            data (Union[pd.DataFrame, DataFrame]): The data to be included in the `DataComponent`.

        Returns:
            DataComponentBuilderFromDataFrame: The current builder instance for chaining.
        """
        self._passport = passport
        return self

    # -------------------------------------------------------------------------------------------- #
    #                                    FILE FORMATS                                              #
    # -------------------------------------------------------------------------------------------- #
    def from_csv(self) -> DataComponentBuilderFromFile:
        """
        Sets the file format to CSV and determines the file path.

        Returns:
            DataComponentBuilderFromDataFrame: The current builder instance for chaining.
        """
        self._file_format = FileFormat.CSV
        return self

    def from_parquet(self) -> DataComponentBuilderFromFile:
        """
        Sets the file format to Parquet and determines the file path.

        Returns:
            DataComponentBuilderFromDataFrame: The current builder instance for chaining.
        """
        self._file_format = FileFormat.PARQUET
        return self

    # -------------------------------------------------------------------------------------------- #
    #                                 DATA FRAME TYPE                                              #
    # -------------------------------------------------------------------------------------------- #
    def to_pandas(self) -> DataComponentBuilderFromFile:
        """
        Sets the file format to CSV and determines the file path.

        Returns:
            DataComponentBuilderFromDataFrame: The current builder instance for chaining.
        """
        self._dftype = DFType.PANDAS
        return self

    def to_spark(self) -> DataComponentBuilderFromFile:
        """
        Sets the file format to Parquet and determines the file path.

        Returns:
            DataComponentBuilderFromDataFrame: The current builder instance for chaining.
        """
        self._dftype = DFType.SPARK
        return self

    # -------------------------------------------------------------------------------------------- #
    #                                       BUILD                                                  #
    # -------------------------------------------------------------------------------------------- #
    def build(self) -> DataComponentBuilderFromFile:
        self._validate()

        self._file_format = self._get_file_format()

        self._filepath = self._get_filepath()

        self._dataframe = self._read_data()

        self._data_component = DataComponent(
            passport=self._passport,
            filepath=self._filepath,
            file_format=self._file_format,
            dataframe=self._dataframe,
        )

        return self

    def _read_data(self) -> Union[pd.DataFrame, DataFrame]:

        if self._dftype == DFType.SPARK:
            return self._fao.read(
                filepath=self._source_filepath, dftype=self._dftype, spark=self._spark
            )
        else:
            return self._fao.read(filepath=self._source_filepath, dftype=self._dftype)

    def _get_file_format(self) -> FileFormat:
        """Infers the file type from the file path if not provided."""
        if self._file_format is None:
            filetype = self._ftd.get_file_type(path=self._source_filepath)
            file_format = FileFormat.from_value(value=filetype)
            return file_format
        else:
            return self._file_format

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
        # Ensure a passport is provided
        if not isinstance(self._passport, DatasetPassport):
            msg = "A DataComponent requires a DatasetPassport object for the Dataset to which this component belongs."
            self._logger.error(msg)
            raise TypeError(msg)

        # Ensure a source filepath was provided and it exists
        if self._source_filepath is None:
            msg = "A source filepath must be provided."
            self._logger.error(msg)
            raise TypeError(msg)
        if not os.path.exists(self._source_filepath):
            msg = f"Source file not found at {self._source_filepath}"
            self._logger.error(msg)
            raise FileNotFoundError(msg)

        # Ensure a spark session is included if the dataframe type is spark.
        if not isinstance(self._spark, SparkSession) and self._dftype == DFType.SPARK:
            msg = "For Spark DataFrames and Spark session is required. Since no spark session was provided, a default is being provided. You're welcome!"
            self._logger.info(msg)
            self._spark = self._spark_session_pool.get_spark_session()
