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
# Modified   : Monday December 23rd 2024 08:28:04 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Module"""
from __future__ import annotations

from enum import Enum
from typing import Optional, Type, Union

import pandas as pd
from pyspark.sql import DataFrame

from discover.asset.base import Asset
from discover.asset.dataset.dataframe import DataFrameStructure
from discover.core.asset import AssetType
from discover.core.flow import PhaseDef, StageDef
from discover.infra.config.app import AppConfigReader
from discover.infra.persist.dataframe.pandas import (
    DataFrameReader as PandasDataFrameReader,
)
from discover.infra.persist.dataframe.spark import (
    DataFrameReader as SparkDataFrameReader,
)
from discover.infra.service.spark.pool import SparkSessionPool


# ------------------------------------------------------------------------------------------------ #
#                                     FILE FORMATS                                                 #
# ------------------------------------------------------------------------------------------------ #
class FileFormat(Enum):
    CSV = "csv"
    PARQUET = "parquet"


# ------------------------------------------------------------------------------------------------ #
#                                        DATASET                                                   #
# ------------------------------------------------------------------------------------------------ #
class Dataset(Asset):

    __ASSET_TYPE = AssetType.DATASET

    def __init__(
        self,
        phase: PhaseDef,
        stage: StageDef,
        name: str,
        spark_session_pool: SparkSessionPool,
        description: Optional[str] = None,
        data: Optional[Union[pd.DataFrame, DataFrame]] = None,
        dataframe_structure: DataFrameStructure = DataFrameStructure.SPARK,
        file_format: FileFormat = FileFormat.PARQUET,
        source: Optional[Dataset] = None,
        parent: Optional[Dataset] = None,
        config_reader_cls: Type[AppConfigReader] = AppConfigReader,
    ) -> None:
        super().__init__(
            asset_type=self.__ASSET_TYPE,
            name=name,
            phase=phase,
            stage=stage,
            description=description,
        )
        self._data = data
        self._description = (
            self._description
            or f"Dataset asset {self._asset_id} - {self.name} created by {source} on {self._created.strftime('%Y-%m-%d')} at {self._created.strftime('H:%M:%S')}"
        )
        self._dataframe_structure = dataframe_structure
        self._file_format = file_format
        self._source = source
        self._parent = parent

        self._config_reader = config_reader_cls()
        self._fal_config = self._config_reader.get_config(
            section="fal", namespace=False
        )

        self._spark_session_pool = spark_session_pool

        # Set after instantiation by the DatasetFactory
        self._asset_id = None
        self._filepath = None

        self._is_composite = False

    # --------------------------------------------------------------------------------------------- #
    #                                  DATASET PROPERTIES                                           #
    # --------------------------------------------------------------------------------------------- #
    @property
    def file_format(self) -> FileFormat:
        return self._file_format

    @property
    def dataframe_structure(self) -> DataFrameStructure:
        return self._dataframe_structure

    @property
    def source(self) -> Dataset:
        return self._source

    @property
    def parent(self) -> Dataset:
        return self._parent

    # --------------------------------------------------------------------------------------------- #
    #                                      SERIALIZATION                                            #
    # --------------------------------------------------------------------------------------------- #
    def __getstate__(self) -> dict:
        """
        Prepares the object's state for serialization.

        This method converts the object's attributes into a dictionary
        that can be serialized, ensuring compatibility with serialization
        libraries and allowing the asset's state to be stored or transmitted.

        Returns:
            dict: A dictionary representation of the object's state.
        """
        # Exclude non-serializable or private attributes if necessary
        state = self.__dict__.copy()
        state["_data"] = None  # Exclude data
        return state

    def __setstate__(self, state) -> None:
        """
        Restores the object's state during deserialization.

        Args:
            state (dict): The state dictionary to restore.
        """
        self.__dict__.update(state)

    # --------------------------------------------------------------------------------------------- #
    #                                          SPARK                                                #
    # --------------------------------------------------------------------------------------------- #
    def stop_spark_session(self, nlp: bool = False) -> None:
        self._spark_session_pool.stop(nlp=nlp)

    # --------------------------------------------------------------------------------------------- #
    #                                      EXTRACT DATA                                             #
    # --------------------------------------------------------------------------------------------- #
    def to_pandas(self) -> pd.DataFrame:
        """Returns the dataset data in a Pandas DataFrame.

        Returns:
            pd.DataFrame: The dataset in Pandas format.
        """
        read_kwargs = self._fal_config["pandas"][self._fal_config["format"]][
            self._file_format.value
        ]["read_kwargs"]

        return self._read_data_pandas(filepath=self._filepath, **read_kwargs)

    # --------------------------------------------------------------------------------------------- #
    def to_spark(self) -> DataFrame:
        """Returns the dataset data as a Spark DataFrame.

        Returns:
            DataFrame: The dataset in Spark format.
        """
        read_kwargs = self._fal_config["spark"][self._fal_config["format"]][
            self._file_format.value
        ]["read_kwargs"]

        return self._read_data_spark(filepath=self._filepath, **read_kwargs)

    # --------------------------------------------------------------------------------------------- #
    def to_sparknlp(self) -> DataFrame:
        """Converts the dataset to a SparkNLP DataFrame.

        Returns:
            DataFrame: The dataset in SparkNLP format.
        """
        read_kwargs = self._fal_config["spark"][self._fal_config["format"]][
            self._file_format.value
        ]["read_kwargs"]

        return self._read_data_sparknlp(filepath=self._filepath, **read_kwargs)

    # -------------------------------------------------------------------------------------------- #
    #                                      READ DATA                                               #
    # -------------------------------------------------------------------------------------------- #
    def _read_data_pandas(self, filepath: str, **kwargs) -> pd.DataFrame:

        # Check if the data has already been loaded in the desired structure.
        if self._dataframe_structure == DataFrameStructure.PANDAS and isinstance(
            self._data, (pd.DataFrame, pd.core.Frame.DataFrame)
        ):
            return self._data
        else:
            if self._file_format == FileFormat.CSV:
                return PandasDataFrameReader.csv(filepath=filepath, **kwargs)
            elif self._file_format == FileFormat.PARQUET:
                return PandasDataFrameReader.parquet(filepath=filepath, **kwargs)
            else:
                raise ValueError(f"Invalid file format {self._file_format}")

    # -------------------------------------------------------------------------------------------- #
    def _read_data_spark(self, filepath: str, **kwargs) -> DataFrame:
        # Check if the data has already been loaded in the desired structure.
        if self._dataframe_structure == DataFrameStructure.SPARK and isinstance(
            self._data, DataFrame
        ):
            return self._data
        else:
            spark = self._spark_session_pool.spark
            if self._file_format == FileFormat.CSV:
                return SparkDataFrameReader.csv(
                    filepath=filepath, spark=spark, **kwargs
                )
            elif self._file_format == FileFormat.PARQUET:
                return SparkDataFrameReader.parquet(
                    filepath=filepath, spark=spark, **kwargs
                )
            else:
                raise ValueError(f"Invalid file format {self._file_format}")

    # -------------------------------------------------------------------------------------------- #
    def _read_data_sparknlp(self, filepath: str, **kwargs) -> DataFrame:
        # Check if the data has already been loaded in the desired structure.
        if self._dataframe_structure == DataFrameStructure.SPARKNLP and isinstance(
            self._data, DataFrame
        ):
            return self._data
        else:
            spark = self._spark_session_pool.sparknlp
            if self._file_format == FileFormat.CSV:
                return SparkDataFrameReader.csv(
                    filepath=filepath, spark=spark, **kwargs
                )
            elif self._file_format == FileFormat.PARQUET:
                return SparkDataFrameReader.parquet(
                    filepath=filepath, spark=spark, **kwargs
                )
            else:
                raise ValueError(f"Invalid file format {self._file_format}")
