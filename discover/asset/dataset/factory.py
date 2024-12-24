#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/asset/dataset/factory.py                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday December 18th 2024 03:26:09 pm                                            #
# Modified   : Monday December 23rd 2024 10:01:50 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Factory Module"""
import os
from typing import Optional, Type, Union

import pandas as pd
from dependency_injector.wiring import Provide, inject
from pydantic import validate_call
from pyspark.sql import DataFrame

from discover.asset.base import Asset, Factory
from discover.asset.dataset.dataframe import DataFrameStructure
from discover.asset.dataset.dataset import Dataset, FileFormat
from discover.container import DiscoverContainer
from discover.core.flow import PhaseDef, StageDef
from discover.infra.persist.dataframe.pandas import PandasFAO
from discover.infra.persist.dataframe.spark import SparkFAO
from discover.infra.service.spark.pool import SparkSessionPool
from discover.infra.workspace.service import WorkspaceService


# ------------------------------------------------------------------------------------------------ #
#                                     DATASET FACTORY                                              #
# ------------------------------------------------------------------------------------------------ #
class DatasetFactory(Factory):
    @inject
    def __init__(
        self,
        workspace_service: WorkspaceService = Provide[
            DiscoverContainer.workspace.service
        ],
        spark_session_pool: SparkSessionPool = Provide[
            DiscoverContainer.spark.session_pool
        ],
        pandas_fao_cls: Type[PandasFAO] = PandasFAO,
        spark_fao_cls: Type[SparkFAO] = SparkFAO,
        fal_config: dict = Provide[DiscoverContainer.config.fal],
    ) -> None:
        self._workspace_service = workspace_service
        self._spark_session_pool = spark_session_pool
        self._pandas_fao = pandas_fao_cls
        self._spark_fao = spark_fao_cls
        self._fal_config = fal_config  # File access layer config

    def set_filepath(self, asset: Asset) -> Asset:
        if asset.asset_id is None:
            asset = self.set_asset_id(asset=asset)
        directory = self._workspace_service.files
        basename = asset.asset_id
        filext = asset.file_format.value
        filename = f"{basename}.{filext}"
        filepath = os.path.join(directory, filename)
        setattr(asset, "_filepath", filepath)
        return asset

    # -------------------------------------------------------------------------------------------- #
    #                                 FROM PARQUET FILE                                            #
    # -------------------------------------------------------------------------------------------- #
    @validate_call(config=dict(arbitrary_types_allowed=True))
    def from_parquet_file(
        self,
        filepath: str,
        phase: PhaseDef,
        stage: StageDef,
        name: str,
        description: Optional[str] = None,
        dataframe_structure: DataFrameStructure = DataFrameStructure.PANDAS,
    ) -> Dataset:
        return self._from_file(
            filepath=filepath,
            phase=phase,
            stage=stage,
            name=name,
            dataframe_structure=dataframe_structure,
            file_format=FileFormat.PARQUET,
        )

    # -------------------------------------------------------------------------------------------- #
    #                                   FROM CSV FILE                                              #
    # -------------------------------------------------------------------------------------------- #
    @validate_call(config=dict(arbitrary_types_allowed=True))
    def from_csv_file(
        self,
        filepath: str,
        phase: PhaseDef,
        stage: StageDef,
        name: str,
        description: Optional[str] = None,
        dataframe_structure: DataFrameStructure = DataFrameStructure.PANDAS,
    ) -> Dataset:
        return self._from_file(
            filepath=filepath,
            phase=phase,
            stage=stage,
            name=name,
            dataframe_structure=dataframe_structure,
            file_format=FileFormat.CSV,
        )

    # -------------------------------------------------------------------------------------------- #
    def _from_file(
        self,
        filepath: str,
        phase: PhaseDef,
        stage: StageDef,
        name: str,
        description: Optional[str] = None,
        dataframe_structure: DataFrameStructure = DataFrameStructure.PANDAS,
        file_format: FileFormat = FileFormat.PARQUET,
    ) -> Dataset:
        data = self._read_data(
            filepath=filepath,
            dataframe_structure=dataframe_structure,
            file_format=file_format,
        )
        dataset = Dataset(
            phase=phase,
            stage=stage,
            name=name,
            description=description,
            spark_session_pool=self._spark_session_pool,
            data=data,
            dataframe_structure=dataframe_structure,
            file_format=file_format,
        )
        return self._register_dataset(dataset=dataset)

    # -------------------------------------------------------------------------------------------- #
    #                                FROM PANDAS DATAFRAME                                         #
    # -------------------------------------------------------------------------------------------- #
    @validate_call(config=dict(arbitrary_types_allowed=True))
    def from_pandas(
        self,
        phase: PhaseDef,
        stage: StageDef,
        name: str,
        data: pd.DataFrame,
        description: Optional[str] = None,
        file_format: FileFormat = FileFormat.PARQUET,
    ) -> Dataset:
        return self._from_df(
            phase=phase,
            stage=stage,
            name=name,
            data=data,
            description=description,
            dataframe_structure=DataFrameStructure.PANDAS,
            file_format=file_format,
        )

    # -------------------------------------------------------------------------------------------- #
    @validate_call(config=dict(arbitrary_types_allowed=True))
    def from_spark(
        self,
        phase: PhaseDef,
        stage: StageDef,
        name: str,
        data: DataFrame,
        description: Optional[str] = None,
        file_format: FileFormat = FileFormat.PARQUET,
    ) -> Dataset:
        return self._from_df(
            phase=phase,
            stage=stage,
            name=name,
            data=data,
            description=description,
            dataframe_structure=DataFrameStructure.SPARK,
            file_format=file_format,
        )

    # -------------------------------------------------------------------------------------------- #
    @validate_call(config=dict(arbitrary_types_allowed=True))
    def from_sparknlp(
        self,
        phase: PhaseDef,
        stage: StageDef,
        name: str,
        data: DataFrame,
        description: Optional[str] = None,
        file_format: FileFormat = FileFormat.PARQUET,
    ) -> Dataset:
        return self._from_df(
            phase=phase,
            stage=stage,
            name=name,
            data=data,
            description=description,
            dataframe_structure=DataFrameStructure.SPARKNLP,
            file_format=file_format,
        )

    # -------------------------------------------------------------------------------------------- #
    #                                       HELPERS                                                #
    # -------------------------------------------------------------------------------------------- #
    @validate_call(config=dict(arbitrary_types_allowed=True))
    def _from_df(
        self,
        phase: PhaseDef,
        stage: StageDef,
        name: str,
        data: pd.DataFrame,
        description: Optional[str] = None,
        dataframe_structure: DataFrameStructure = DataFrameStructure.SPARK,
        file_format: FileFormat = FileFormat.PARQUET,
        **kwargs,
    ) -> Dataset:
        dataset = Dataset(
            phase=phase,
            stage=stage,
            name=name,
            data=data,
            description=description,
            dataframe_structure=dataframe_structure,
            file_format=file_format,
        )

        return self._register_dataset(dataset=dataset)

    # -------------------------------------------------------------------------------------------- #
    #                                  REGISTER DATASET                                            #
    # -------------------------------------------------------------------------------------------- #
    def _register_dataset(self, dataset: Dataset) -> Dataset:
        dataset = self.set_asset_id(asset=dataset)
        dataset = self.set_filepath(asset=dataset)
        return dataset

    # -------------------------------------------------------------------------------------------- #
    #                                       READERS                                                #
    # -------------------------------------------------------------------------------------------- #
    def _read_data(
        self,
        filepath: str,
        dataframe_structure: DataFrameStructure,
        file_format: FileFormat,
    ) -> Union[pd.DataFrame, DataFrame]:
        if dataframe_structure == DataFrameStructure.PANDAS:
            return self._read_pandas(filepath=filepath, file_format=file_format)
        elif dataframe_structure == DataFrameStructure.SPARK:
            return self._read_spark(filepath=filepath, file_format=file_format)
        elif dataframe_structure == DataFrameStructure.SPARKNLP:
            return self._read_sparknlp(filepath=filepath, file_format=file_format)
        else:
            raise ValueError(f"Unrecognized dataframe_structure: {dataframe_structure}")

    # -------------------------------------------------------------------------------------------- #
    def _read_pandas(self, filepath: str, file_format: FileFormat) -> pd.DataFrame:
        # Obtain read and write kwargs for the dataframe_structure and file_format.
        read_kwargs = self._fal_config["pandas"][file_format.value]["read_kwargs"]

        return self._pandas_fao.read(
            filepath=filepath, file_format=file_format, **read_kwargs
        )

    # -------------------------------------------------------------------------------------------- #
    def _read_spark(self, filepath: str, file_format: FileFormat) -> DataFrame:
        # Obtain read and write kwargs for the dataframe_structure and file_format.
        read_kwargs = self._fal_config["spark"][file_format.value]["read_kwargs"]
        # Get the spark session
        spark = self._spark_session_pool.spark
        return self._spark_fao.read(
            filepath=filepath, spark=spark, file_format=file_format, **read_kwargs
        )

    # -------------------------------------------------------------------------------------------- #
    def _read_sparknlp(self, filepath: str, file_format: FileFormat) -> DataFrame:
        # Obtain read and write kwargs for the dataframe_structure and file_format.
        read_kwargs = self._fal_config["spark"][file_format.value]["read_kwargs"]
        # Get the spark session
        spark = self._spark_session_pool.sparknlp
        return self._spark_fao.read(
            filepath=filepath, spark=spark, file_format=file_format, **read_kwargs
        )

    # -------------------------------------------------------------------------------------------- #
    #                                       WRITERS                                                #
    # -------------------------------------------------------------------------------------------- #
    def _write_data(
        self,
        filepath: str,
        data: Union[pd.DataFrame, DataFrame],
        dataframe_structure: DataFrameStructure,
        file_format: FileFormat,
    ) -> None:
        if dataframe_structure == DataFrameStructure.PANDAS:
            self._write_pandas(filepath=filepath, data=data, file_format=file_format)
        elif dataframe_structure == DataFrameStructure.SPARK:
            self._write_spark(filepath=filepath, data=data, file_format=file_format)
        elif dataframe_structure == DataFrameStructure.SPARKNLP:
            self._write_sparknlp(filepath=filepath, data=data, file_format=file_format)
        else:
            raise ValueError(f"Unrecognized dataframe_structure: {dataframe_structure}")

    # -------------------------------------------------------------------------------------------- #
    def _write_pandas(
        self, filepath: str, data: pd.DataFrame, file_format: FileFormat
    ) -> None:
        # Obtain write and write kwargs for the dataframe_structure and file_format.
        write_kwargs = self._fal_config["pandas"][file_format.value]["write_kwargs"]

        return self._pandas_fao.write(
            filepath=filepath, data=data, file_format=file_format, **write_kwargs
        )

    # -------------------------------------------------------------------------------------------- #
    def _write_spark(
        self, filepath: str, data: DataFrame, file_format: FileFormat
    ) -> None:
        # Obtain write and write kwargs for the dataframe_structure and file_format.
        write_kwargs = self._fal_config["spark"][file_format.value]["write_kwargs"]

        return self._spark_fao.write(
            filepath=filepath, data=data, file_format=file_format, **write_kwargs
        )

    # -------------------------------------------------------------------------------------------- #
    def _write_sparknlp(
        self, filepath: str, data: DataFrame, file_format: FileFormat
    ) -> None:
        self._write_spark(filepath=filepath, data=data, file_format=file_format)
