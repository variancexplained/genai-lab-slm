#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/asset/dataset.py                                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 01:35:04 am                                              #
# Modified   : Tuesday December 24th 2024 07:35:36 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Module"""
from __future__ import annotations

import os
from enum import Enum
from typing import Optional, Union

import pandas as pd
from dependency_injector.wiring import Provide, inject
from pydantic import validate_call
from pyspark.sql import DataFrame

from discover.asset.base import Asset, Factory
from discover.container import DiscoverContainer
from discover.core.asset import AssetType
from discover.core.file import FileFormat
from discover.core.flow import PhaseDef, StageDef
from discover.infra.exception.file import FileIOException
from discover.infra.persist.dataframe.base import DataFrameReader, DataFrameWriter
from discover.infra.persist.dataframe.pandas import (
    DataFrameReader as PandasDataFrameReader,
)
from discover.infra.persist.dataframe.pandas import (
    DataFrameWriter as PandasDataFrameWriter,
)
from discover.infra.persist.dataframe.spark import (
    DataFrameReader as SparkDataFrameReader,
)
from discover.infra.persist.dataframe.spark import (
    DataFrameWriter as SparkDataFrameWriter,
)
from discover.infra.service.spark.pool import SparkSessionPool
from discover.infra.utils.file.copy import Copy
from discover.infra.workspace.service import WorkspaceService


# ------------------------------------------------------------------------------------------------ #
#                                  DATAFRAME STRUCTURE                                             #
# ------------------------------------------------------------------------------------------------ #
class DataFrameStructure(Enum):
    """
    Enum representing different DataFrame structures and their associated attributes.

    Each DataFrame structure is defined by:
        - A string identifier.
        - A flag indicating support for distributed computing.
        - A flag indicating support for NLP-specific functionality.
        - A reader class for reading data into the structure.
        - A writer class for writing data from the structure.

    Attributes:
        distributed (bool): Whether the DataFrame type supports distributed computing.
        nlp (bool): Whether the DataFrame type supports NLP-specific functionality.
        reader (DataFrameReader): The class responsible for reading data into the structure.
        writer (DataFrameWriter): The class responsible for writing data from the structure.
    """

    PANDAS = ("pandas", PandasDataFrameReader, PandasDataFrameWriter)
    SPARK = ("spark", SparkDataFrameReader, SparkDataFrameWriter)
    SPARKNLP = ("sparknlp", SparkDataFrameReader, SparkDataFrameWriter)

    def __new__(
        cls,
        value: str,
        reader: DataFrameReader,
        writer: DataFrameWriter,
    ) -> DataFrameStructure:
        """
        Creates a new DataFrameStructure instance.

        Args:
            value (str): The string identifier of the DataFrame structure (e.g., "pandas").
            distributed (bool): Whether the structure supports distributed computing.
            nlp (bool): Whether the structure supports NLP-specific functionality.
            reader (DataFrameReader): The class responsible for reading data.
            writer (DataFrameWriter): The class responsible for writing data.

        Returns:
            DataFrameStructure: A new instance of the DataFrameStructure enum.
        """
        obj = object.__new__(cls)
        obj._value_ = value
        obj._reader = reader
        obj._writer = writer
        return obj

    @property
    def reader(self) -> DataFrameReader:
        """
        Returns the reader class associated with the DataFrame structure.

        Returns:
            DataFrameReader: The reader class for the structure.
        """
        return self._reader

    @property
    def writer(self) -> DataFrameWriter:
        """
        Returns the writer class associated with the DataFrame structure.

        Returns:
            DataFrameWriter: The writer class for the structure.
        """
        return self._writer

    @classmethod
    def from_value(cls, value: str) -> DataFrameStructure:
        """
        Finds the enum member based on its string identifier.

        Args:
            value (str): The string identifier of the DataFrame structure (e.g., "pandas").

        Returns:
            DataFrameStructure: The matching enum member.

        Raises:
            ValueError: If no matching enum member is found.
        """
        value = value.lower()
        for member in cls:
            if member._value_ == value:
                return member
        raise ValueError(f"No matching {cls.__name__} for {value}")


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
        fal_config: dict,
        description: Optional[str] = None,
        data: Optional[Union[pd.DataFrame, DataFrame]] = None,
        dataframe_structure: Optional[DataFrameStructure] = None,
        file_format: FileFormat = FileFormat.PARQUET,
        source: str = None,  # Asset id of source
        parent: str = None,  # Asset id of parent
    ) -> None:
        super().__init__(
            asset_type=self.__ASSET_TYPE,
            name=name,
            phase=phase,
            stage=stage,
            description=description,
        )
        self._description = (
            self._description
            or f"Dataset asset {self._asset_id} - {self.name} created by {source} on {self._created.strftime('%Y-%m-%d')} at {self._created.strftime('H:%M:%S')}"
        )

        self._data = data
        self._dataframe_structure = dataframe_structure
        self._file_format = file_format

        self._source = source
        self._parent = parent

        self._fal_config = fal_config

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

    @property
    def filepath(self) -> str:
        return self._filepath

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
    #                                      EXTRACT DATA                                             #
    # --------------------------------------------------------------------------------------------- #
    def to_pandas(self) -> pd.DataFrame:
        """Converts (if necessary) and returns the underlying data as a Pandas DataFrame.

        Returns:
            pd.DataFrame: The dataset in Pandas format.
        """
        if self._dataframe_structure == DataFrameStructure.PANDAS and isinstance(
            self._data, (pd.DataFrame, pd.core.frame.DataFrame)
        ):
            return self._data
        else:
            # Reading dataframes is often faster than converting spark to pandas.
            read_kwargs = self._fal_config["pandas"][self._file_format.value][
                "read_kwargs"
            ]
            try:
                self._data = self._read_data_pandas(
                    filepath=self._filepath, **read_kwargs
                )
                self._dataframe_structure = DataFrameStructure.PANDAS
                return self._data
            except (
                FileIOException
            ):  # This can occur if the data is written as a csv directory
                # Obtain a spark dataframe and convert it
                data = self.to_spark()
                self._data = data.toPandas()
                self._dataframe_structure = DataFrameStructure.PANDAS
                return self._data
            except Exception as e:
                raise Exception(
                    f"Exception returning data at {self._filepath} as a pandas DataFrame"
                ) from e

    # --------------------------------------------------------------------------------------------- #
    def to_spark(self) -> DataFrame:
        """Converts (if necessary) and returns the underlying data as a Spark DataFrame.

        Returns:
            DataFrame: The dataset in Spark format.
        """
        if self._dataframe_structure == DataFrameStructure.SPARK and isinstance(
            self._data, DataFrame
        ):
            return self._data
        else:
            read_kwargs = self._fal_config["spark"][self._file_format.value][
                "read_kwargs"
            ]
            self._data = self._read_data_spark(filepath=self._filepath, **read_kwargs)
            self._dataframe_structure = DataFrameStructure.SPARK
            return self._data

    # --------------------------------------------------------------------------------------------- #
    def to_sparknlp(self) -> DataFrame:
        """Converts (if necessary) and returns the underlying data as a SparkNLP DataFrame.

        Returns:
            DataFrame: The dataset in SparkNLP format.
        """
        if self._dataframe_structure == DataFrameStructure.SPARKNLP and isinstance(
            self._data, DataFrame
        ):
            return self._data
        else:
            read_kwargs = self._fal_config["spark"][self._file_format.value][
                "read_kwargs"
            ]
            self._data = self._read_data_sparknlp(
                filepath=self._filepath, **read_kwargs
            )
            self._dataframe_structure = DataFrameStructure.SPARKNLP
            return self._data

    # -------------------------------------------------------------------------------------------- #
    #                                      READ DATA                                               #
    # -------------------------------------------------------------------------------------------- #
    def _read_data(
        self,
        filepath: str,
        dataframe_structure: DataFrameStructure,
        **kwargs,
    ) -> Union[pd.DataFrame, DataFrame]:
        if dataframe_structure == DataFrameStructure.PANDAS:
            return self._read_data_pandas(filepath=filepath, **kwargs)
        elif dataframe_structure == DataFrameStructure.SPARK:
            return self._read_data_spark(filepath=filepath, **kwargs)
        elif dataframe_structure == DataFrameStructure.SPARKNLP:
            return self._read_data_sparknlp(filepath=filepath, **kwargs)
        else:
            raise ValueError(f"Unrecognized dataframe_structure: {dataframe_structure}")

    # -------------------------------------------------------------------------------------------- #
    def _read_data_pandas(self, filepath: str, **kwargs) -> pd.DataFrame:

        if self._file_format == FileFormat.CSV:
            return DataFrameStructure.PANDAS.reader.csv(filepath=filepath, **kwargs)
        elif self._file_format == FileFormat.PARQUET:
            return DataFrameStructure.PANDAS.reader.parquet(filepath=filepath, **kwargs)
        else:
            raise ValueError(f"Invalid file format {self._file_format}")

    # -------------------------------------------------------------------------------------------- #
    @inject
    def _read_data_spark(
        self,
        filepath: str,
        spark_session_pool: SparkSessionPool = Provide[
            DiscoverContainer.spark.session_pool
        ],
        **kwargs,
    ) -> DataFrame:
        spark = spark_session_pool.spark
        if self._file_format == FileFormat.CSV:
            return DataFrameStructure.SPARK.reader.csv(
                filepath=filepath, spark=spark, **kwargs
            )
        elif self._file_format == FileFormat.PARQUET:
            return DataFrameStructure.SPARK.reader.parquet(
                filepath=filepath, spark=spark, **kwargs
            )
        else:
            raise ValueError(f"Invalid file format {self._file_format}")

    # -------------------------------------------------------------------------------------------- #
    @inject
    def _read_data_sparknlp(
        self,
        filepath: str,
        spark_session_pool: SparkSessionPool = Provide[
            DiscoverContainer.spark.session_pool
        ],
        **kwargs,
    ) -> DataFrame:
        spark = spark_session_pool.sparknlp
        if self._file_format == FileFormat.CSV:
            return DataFrameStructure.SPARKNLP.reader.csv(
                filepath=filepath, spark=spark, **kwargs
            )
        elif self._file_format == FileFormat.PARQUET:
            return DataFrameStructure.SPARKNLP.reader.parquet(
                filepath=filepath, spark=spark, **kwargs
            )
        else:
            raise ValueError(f"Invalid file format {self._file_format}")


# ------------------------------------------------------------------------------------------------ #
#                                     DATASET FACTORY                                              #
# ------------------------------------------------------------------------------------------------ #
class DatasetFactory(Factory):

    @inject
    def __init__(
        self,
        config: dict = Provide[DiscoverContainer.config],
        workspace_service: WorkspaceService = Provide[
            DiscoverContainer.workspace.service
        ],
    ) -> None:
        self._workspace_service = workspace_service
        self._fal_config = config["fal"]  # File access layer config

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
        dataframe_structure: Optional[DataFrameStructure] = None,
    ) -> Dataset:
        return self._from_file(
            filepath=filepath,
            phase=phase,
            stage=stage,
            name=name,
            description=description,
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
        dataframe_structure: Optional[DataFrameStructure] = None,
    ) -> Dataset:
        return self._from_file(
            filepath=filepath,
            phase=phase,
            stage=stage,
            name=name,
            description=description,
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
        dataframe_structure: Optional[DataFrameStructure] = None,
        file_format: FileFormat = FileFormat.PARQUET,
    ) -> Dataset:

        dataset = Dataset(
            phase=phase,
            stage=stage,
            name=name,
            description=description,
            fal_config=self._fal_config,
            dataframe_structure=dataframe_structure,
            file_format=file_format,
        )

        dataset = self.set_asset_id(asset=dataset)
        dataset = self.set_filepath(asset=dataset)

        copy = Copy()
        copy(source=filepath, target=dataset.filepath, overwrite=False)

        self._register_dataset(dataset=dataset)
        return dataset

    # -------------------------------------------------------------------------------------------- #
    #                                FROM PANDAS DATAFRAME                                         #
    # -------------------------------------------------------------------------------------------- #
    @validate_call(config=dict(arbitrary_types_allowed=True))
    def from_pandas_dataframe(
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
    #                                 FROM SPARK DATAFRAME                                         #
    # -------------------------------------------------------------------------------------------- #
    @validate_call(config=dict(arbitrary_types_allowed=True))
    def from_spark_dataframe(
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
    #                                FROM SPARK NLP DATAFRAME                                      #
    # -------------------------------------------------------------------------------------------- #
    @validate_call(config=dict(arbitrary_types_allowed=True))
    def from_sparknlp_dataframe(
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
    @validate_call(config=dict(arbitrary_types_allowed=True))
    def _from_df(
        self,
        phase: PhaseDef,
        stage: StageDef,
        name: str,
        data: Union[pd.DataFrame, DataFrame],
        dataframe_structure: DataFrameStructure,
        description: Optional[str] = None,
        file_format: FileFormat = FileFormat.PARQUET,
        **kwargs,
    ) -> Dataset:
        dataset = Dataset(
            phase=phase,
            stage=stage,
            name=name,
            data=data,
            description=description,
            fal_config=self._fal_config,
            dataframe_structure=dataframe_structure,
            file_format=file_format,
        )

        dataset = self.set_asset_id(asset=dataset)
        dataset = self.set_filepath(asset=dataset)

        self._write_data(
            filepath=dataset.filepath,
            data=data,
            dataframe_structure=dataframe_structure,
            file_format=file_format,
        )

        self._register_dataset(dataset=dataset)
        return dataset

    # -------------------------------------------------------------------------------------------- #
    #                                  REGISTER DATASET                                            #
    # -------------------------------------------------------------------------------------------- #
    def _register_dataset(self, dataset: Dataset) -> None:
        self._workspace_service.dataset_repo.add(asset=dataset)

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

        # Obtain the pandas reader for the file format
        if file_format == FileFormat.CSV:
            reader = DataFrameStructure.PANDAS.reader.csv
        elif file_format == FileFormat.PARQUET:
            reader = DataFrameStructure.PANDAS.reader.parquet
        else:
            raise ValueError(f"Unrecognized file format {file_format}")

        # Read and return the data
        return reader(filepath=filepath, **read_kwargs)

    # -------------------------------------------------------------------------------------------- #
    @inject
    def _read_spark(
        self,
        filepath: str,
        file_format: FileFormat,
        spark_session_pool: SparkSessionPool = Provide[
            DiscoverContainer.spark.session_pool
        ],
    ) -> DataFrame:
        # Obtain read and write kwargs for the dataframe_structure and file_format.
        read_kwargs = self._fal_config["spark"][file_format.value]["read_kwargs"]

        # Obtain the spark reader for the file format
        if file_format == FileFormat.CSV:
            reader = DataFrameStructure.SPARK.reader.csv
        elif file_format == FileFormat.PARQUET:
            reader = DataFrameStructure.SPARK.reader.parquet
        else:
            raise ValueError(f"Unrecognized file format {file_format}")

        # Read and return the data
        return reader(filepath=filepath, spark=spark_session_pool.spark, **read_kwargs)

    # -------------------------------------------------------------------------------------------- #
    @inject
    def _read_sparknlp(
        self,
        filepath: str,
        file_format: FileFormat,
        spark_session_pool: SparkSessionPool = Provide[
            DiscoverContainer.spark.session_pool
        ],
    ) -> DataFrame:
        # Obtain read and write kwargs for the dataframe_structure and file_format.
        read_kwargs = self._fal_config["spark"][file_format.value]["read_kwargs"]

        # Obtain the sparknlp reader for the file format
        if file_format == FileFormat.CSV:
            reader = DataFrameStructure.SPARKNLP.reader.csv
        elif file_format == FileFormat.PARQUET:
            reader = DataFrameStructure.SPARKNLP.reader.parquet
        else:
            raise ValueError(f"Unrecognized file format {file_format}")

        # Read and return the data
        return reader(
            filepath=filepath, spark=spark_session_pool.sparknlp, **read_kwargs
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

        # Obtain the pandas writer for the file format
        if file_format == FileFormat.CSV:
            writer = DataFrameStructure.PANDAS.writer.csv
        elif file_format == FileFormat.PARQUET:
            writer = DataFrameStructure.PANDAS.writer.parquet
        else:
            raise ValueError(f"Unrecognized file format {file_format}")

        # Write the data
        return writer(filepath=filepath, data=data, **write_kwargs)

    # -------------------------------------------------------------------------------------------- #
    def _write_spark(
        self, filepath: str, data: DataFrame, file_format: FileFormat
    ) -> None:
        # Obtain write and write kwargs for the dataframe_structure and file_format.
        write_kwargs = self._fal_config["spark"][file_format.value]["write_kwargs"]

        # Obtain the spark writer for the file format
        if file_format == FileFormat.CSV:
            writer = DataFrameStructure.SPARK.writer.csv
        elif file_format == FileFormat.PARQUET:
            writer = DataFrameStructure.SPARK.writer.parquet
        else:
            raise ValueError(f"Unrecognized file format {file_format}")

        # Write the data
        return writer(filepath=filepath, data=data, **write_kwargs)

    # -------------------------------------------------------------------------------------------- #
    def _write_sparknlp(
        self, filepath: str, data: DataFrame, file_format: FileFormat
    ) -> None:
        self._write_spark(filepath=filepath, data=data, file_format=file_format)
