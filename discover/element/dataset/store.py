#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/element/dataset/store.py                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 01:35:16 am                                              #
# Modified   : Tuesday September 24th 2024 02:15:49 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from __future__ import annotations

import os
from ast import List
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional

from discover.core.data_structure import DataStructure
from discover.core.flow import PhaseDef, StageDef
from discover.element.base.store import StorageConfig


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatasetStorageConfig(StorageConfig):
    """
    Base class for dataset storage configuration, handling common properties for different dataset formats and structures.

    This class inherits from `StorageConfig` and defines additional configurations specific to datasets, such as the data structure (e.g., Pandas or Spark) and whether the dataset is partitioned.

    Attributes:
    -----------
    structure : DataStructure
        The data structure of the dataset (e.g., Pandas or Spark). Default is Pandas.
    partitioned : bool
        Indicates whether the dataset is partitioned. Default is True.
    filepath : Optional[str]
        The generated file path where the dataset will be stored. Default is None until assigned.
    row_group_size : Optional[int]
        Optional size for row grouping in the dataset.
    read_kwargs : Dict[str, Any]
        A dictionary of additional arguments for reading the dataset. Defaults to an empty dictionary.
    write_kwargs : Dict[str, Any]
        A dictionary of additional arguments for writing the dataset. Defaults to an empty dictionary.

    Methods:
    --------
    format_filepath(id: int, phase: PhaseDef, stage: StageDef, name: str) -> str:
        Generates a file path for the dataset based on the phase, stage, dataset name, and ID.

    create(id: int, phase: PhaseDef, stage: StageDef, name: str, partitioned: bool = True) -> DatasetStorageConfig:
        Creates a new `DatasetStorageConfig` instance with a generated file path and partitioning settings.
    """

    structure: DataStructure = DataStructure.PANDAS  # Default structure
    partitioned: bool = True  # Indicates if the dataset is partitioned
    filepath: Optional[str] = None  # The generated file path for the dataset
    row_group_size: Optional[int] = None
    read_kwargs: Dict[str, Any] = field(
        default_factory=dict
    )  # Additional read arguments
    write_kwargs: Dict[str, Any] = field(
        default_factory=dict
    )  # Additional write arguments

    @classmethod
    def format_filepath(
        cls, id: int, phase: PhaseDef, stage: StageDef, name: str
    ) -> str:
        """
        Generates a file path for the dataset using the phase, stage, name, and current date.

        Parameters:
        -----------
        id : int
            The ID of the dataset, used to create unique file paths.
        phase : PhaseDef
            The phase in which the dataset is being used (e.g., development, production).
        stage : StageDef
            The stage or step within the phase (e.g., extract, transform, load).
        name : str
            The name of the dataset.

        Returns:
        --------
        str
            A full file path with the phase, stage, dataset name, and timestamp.
        """
        filename = f"{phase.value}_{stage.value}_{name}_dataset_{datetime.now().strftime('%Y%m%d')}-{str(id).zfill(4)}"
        filepath = os.path.join(phase.directory, stage.directory, filename)
        return cls.filepath_service.get_filepath(filepath)

    @classmethod
    def create(
        cls,
        id: int,
        phase: PhaseDef,
        stage: StageDef,
        name: str,
        partitioned: bool = True,
    ) -> DatasetStorageConfig:
        """
        Creates a new `DatasetStorageConfig` instance with a generated file path and partitioning settings.

        Parameters:
        -----------
        id : int
            The ID of the dataset, used to generate a unique file path.
        phase : PhaseDef
            The phase in which the dataset is being used.
        stage : StageDef
            The stage of the dataset's processing lifecycle.
        name : str
            The name of the dataset.
        partitioned : bool, optional
            Whether the dataset should be partitioned. Default is True.

        Returns:
        --------
        DatasetStorageConfig
            A new instance of the `DatasetStorageConfig` with the generated file path and other settings.
        """
        filepath = cls.format_filepath(id=id, phase=phase, stage=stage, name=name)

        # Append .parquet extension if not partitioned
        filepath = filepath + ".parquet" if not partitioned else filepath

        # Return a new DatasetStorageConfig instance with the generated filepath and partitioning settings
        return cls(partitioned=partitioned, filepath=filepath)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class PandasParquetDatasetStorageConfig(DatasetStorageConfig):
    """
    Storage configuration class for non-partitioned Pandas DataFrame parquet files.

    This class inherits from `DatasetStorageConfig` and defines specific configurations for
    handling Pandas DataFrame storage in parquet format, with custom settings for
    file path, compression, row group size, and other parquet-specific options.

    Attributes:
    -----------
    structure : DataStructure
        The data structure is set to Pandas by default.

    Methods:
    --------
    create(id: int, phase: PhaseDef, stage: StageDef, name: str, partitioned: bool = False,
           engine: str = "pyarrow", compression: str = "snappy", index: bool = False,
           row_group_size: int = 128 * 1024 * 1024) -> PandasParquetDatasetStorageConfig:
        Creates a new `PandasParquetDatasetStorageConfig` instance with custom parquet settings
        such as compression, row group size, and whether to include an index.
    """

    structure: DataStructure = DataStructure.PANDAS

    @classmethod
    def create(
        cls,
        id: int,
        phase: PhaseDef,
        stage: StageDef,
        name: str,
        partitioned: bool = False,
        engine: str = "pyarrow",
        compression: str = "snappy",
        index: bool = True,
        row_group_size: int = 128 * 1024 * 1024,  # 128 MB
    ) -> PandasParquetDatasetStorageConfig:
        """
        Creates a new `PandasParquetDatasetStorageConfig` instance configured for
        Pandas DataFrame storage in non-partitioned parquet format.

        Parameters:
        -----------
        id : int
            The dataset's unique identifier, used for generating file paths.
        phase : PhaseDef
            The current phase (e.g., development, production) used to determine the file path.
        stage : StageDef
            The stage or step within the data pipeline (e.g., extract, transform, load).
        name : str
            The name of the dataset.
        partitioned : bool, optional
            Whether the dataset is partitioned. Defaults to False, as parquet for pandas is non-partitioned.
        engine : str, optional
            The engine to use for writing parquet files. Default is "pyarrow".
        compression : str, optional
            Compression format to use for parquet files. Default is "snappy".
        index : bool, optional
            Whether to write the DataFrame index to the parquet file. Default is True.
        row_group_size : int, optional
            Size of each row group in bytes. Default is 128 MB.

        Returns:
        --------
        PandasParquetDatasetStorageConfig
            A new instance of `PandasParquetDatasetStorageConfig` with the specified configuration.
        """
        # Create a base file path that includes phase, stage, name, current date, and ID
        filepath = (
            cls.format_filepath(id=id, phase=phase, stage=stage, name=name) + ".parquet"
        )
        return cls(
            partitioned=partitioned,
            filepath=filepath,
            row_group_size=row_group_size,
            write_kwargs={
                "engine": engine,
                "compression": compression,
                "index": index,
                "row_group_size": row_group_size,
            },
        )


# ------------------------------------------------------------------------------------------------ #
@dataclass
class PandasParquetPartitionedDatasetStorageConfig(DatasetStorageConfig):
    """
    Storage configuration class for partitioned Pandas DataFrame parquet files.

    This class inherits from `DatasetStorageConfig` and defines specific configurations for
    handling partitioned Pandas DataFrame storage in parquet format. It allows customization of
    partition columns, row group size, compression, and handling of existing data.

    Attributes:
    -----------
    structure : DataStructure
        The data structure is set to Pandas by default.

    Methods:
    --------
    create(id: int, phase: PhaseDef, stage: StageDef, name: str, partitioned: bool = True,
           engine: str = "pyarrow", compression: str = "snappy", index: bool = False,
           row_group_size: int = 128 * 1024 * 1024, partition_cols: Optional[List] = None,
           existing_data_behavior: str = "delete_matching") -> PandasParquetPartitionedDatasetStorageConfig:
        Creates a new `PandasParquetPartitionedDatasetStorageConfig` instance configured for
        Pandas DataFrame storage in partitioned parquet format.
    """

    structure: DataStructure = DataStructure.PANDAS

    @classmethod
    def create(
        cls,
        id: int,
        phase: PhaseDef,
        stage: StageDef,
        name: str,
        partitioned: bool = True,
        engine: str = "pyarrow",
        compression: str = "snappy",
        index: bool = True,
        row_group_size: int = 128 * 1024 * 1024,  # 128 MB
        partition_cols: Optional[List] = None,
        existing_data_behavior: str = "delete_matching",
    ) -> PandasParquetPartitionedDatasetStorageConfig:
        """
        Creates a new `PandasParquetPartitionedDatasetStorageConfig` instance configured for
        partitioned Pandas DataFrame storage in parquet format.

        Parameters:
        -----------
        id : int
            The dataset's unique identifier, used for generating file paths.
        phase : PhaseDef
            The current phase (e.g., development, production) used to determine the file path.
        stage : StageDef
            The stage or step within the data pipeline (e.g., extract, transform, load).
        name : str
            The name of the dataset.
        partitioned : bool, optional
            Whether the dataset should be partitioned. Default is True.
        engine : str, optional
            The engine to use for writing parquet files. Default is "pyarrow".
        compression : str, optional
            Compression format to use for parquet files. Default is "snappy".
        index : bool, optional
            Whether to write the DataFrame index to the parquet file. Default is True.
        row_group_size : int, optional
            Size of each row group in bytes. Default is 128 MB.
        partition_cols : Optional[List], optional
            A list of columns to use for partitioning the data. Default is ["category"].
        existing_data_behavior : str, optional
            Defines the behavior for handling existing data (e.g., "delete_matching").
            Default is "delete_matching".

        Returns:
        --------
        PandasParquetPartitionedDatasetStorageConfig
            A new instance of `PandasParquetPartitionedDatasetStorageConfig` with the specified configuration.
        """
        # Create a base file path that includes phase, stage, name, current date, and ID
        filepath = cls.format_filepath(id=id, phase=phase, stage=stage, name=name)

        # Handle mutable default
        if partition_cols is None:
            partition_cols = ["category"]

        # Return a PandasParquetPartitionedDatasetStorageConfig with write configurations for Pandas
        return cls(
            partitioned=partitioned,
            filepath=filepath,
            row_group_size=row_group_size,
            write_kwargs={
                "engine": engine,
                "compression": compression,
                "index": index,
                "row_group_size": row_group_size,
                "partition_cols": partition_cols,
                "existing_data_behavior": existing_data_behavior,
            },
        )


# ------------------------------------------------------------------------------------------------ #
@dataclass
class SparkParquetDatasetStorageConfig(DatasetStorageConfig):
    """
    Storage configuration class for non-partitioned Spark DataFrame parquet files.

    This class inherits from `DatasetStorageConfig` and defines specific configurations for
    handling Spark DataFrame storage in parquet format. It allows customization of the Spark session,
    row group size, write mode, and specific configurations for natural language processing (NLP) workflows.

    Attributes:
    -----------
    structure : DataStructure
        The data structure is set to Spark by default.
    spark_session_name : Optional[str]
        The name of the Spark session to be used for writing the dataset.
    nlp : bool
        A flag indicating whether the dataset is part of an NLP pipeline. Default is False.

    Methods:
    --------
    create(id: int, phase: PhaseDef, stage: StageDef, name: str, spark_session_name: str,
           nlp: bool = False, mode: str = "error", partitioned: bool = False,
           row_group_size: int = 128 * 1024 * 1024) -> SparkParquetDatasetStorageConfig:
        Creates a new `SparkParquetDatasetStorageConfig` instance configured for
        Spark DataFrame storage in non-partitioned parquet format.
    """

    structure: DataStructure = DataStructure.SPARK  # Set the default structure to Spark
    spark_session_name: Optional[str] = None
    nlp: bool = False

    @classmethod
    def create(
        cls,
        id: int,
        phase: PhaseDef,
        stage: StageDef,
        name: str,
        spark_session_name: str,
        nlp: bool = False,
        mode: str = "error",
        partitioned: bool = False,
        row_group_size: int = 128 * 1024 * 1024,  # 128 MB
    ) -> SparkParquetDatasetStorageConfig:
        """
        Creates a new `SparkParquetDatasetStorageConfig` instance configured for
        non-partitioned Spark DataFrame storage in parquet format.

        Parameters:
        -----------
        id : int
            The dataset's unique identifier, used for generating file paths.
        phase : PhaseDef
            The current phase (e.g., development, production) used to determine the file path.
        stage : StageDef
            The stage or step within the data pipeline (e.g., extract, transform, load).
        name : str
            The name of the dataset.
        spark_session_name : str
            The name of the Spark session to be used for the dataset.
        nlp : bool, optional
            A flag indicating whether the dataset is part of an NLP pipeline. Default is False.
        mode : str, optional
            The write mode for Spark DataFrame writes (e.g., "error", "overwrite"). Default is "error".
        partitioned : bool, optional
            Whether the dataset should be partitioned. Default is False for non-partitioned storage.
        row_group_size : int, optional
            Size of each row group in bytes. Default is 128 MB.

        Returns:
        --------
        SparkParquetDatasetStorageConfig
            A new instance of `SparkParquetDatasetStorageConfig` with the specified configuration.
        """
        # Create a base file path that includes phase, stage, name, current date, and ID
        filepath = (
            cls.format_filepath(id=id, phase=phase, stage=stage, name=name) + ".parquet"
        )

        # Return a SparkParquetDatasetStorageConfig with write configurations for Spark
        return cls(
            partitioned=partitioned,
            filepath=filepath,
            nlp=nlp,
            row_group_size=row_group_size,
            spark_session_name=spark_session_name,
            write_kwargs={"mode": mode},
        )


# ------------------------------------------------------------------------------------------------ #
@dataclass
class SparkParquetPartitionedDatasetStorageConfig(DatasetStorageConfig):
    """
    Storage configuration class for partitioned Spark DataFrame parquet files.

    This class inherits from `DatasetStorageConfig` and defines specific configurations for
    handling partitioned Spark DataFrame storage in parquet format. It allows customization of
    the Spark session, partition columns, row group size, and specific configurations for natural language processing (NLP) workflows.

    Attributes:
    -----------
    structure : DataStructure
        The data structure is set to Spark by default.
    spark_session_name : Optional[str]
        The name of the Spark session to be used for writing the dataset.
    nlp : bool
        A flag indicating whether the dataset is part of an NLP pipeline. Default is False.

    Methods:
    --------
    create(id: int, phase: PhaseDef, stage: StageDef, name: str, spark_session_name: str,
           mode: str = "error", nlp: bool = False, partitioned: bool = True,
           partition_cols: Optional[List] = None, row_group_size: int = 128 * 1024 * 1024)
           -> SparkParquetPartitionedDatasetStorageConfig:
        Creates a new `SparkParquetPartitionedDatasetStorageConfig` instance configured for
        partitioned Spark DataFrame storage in parquet format.
    """

    structure: DataStructure = DataStructure.SPARK  # Set the default structure to Spark
    spark_session_name: Optional[str] = None
    nlp: bool = False

    @classmethod
    def create(
        cls,
        id: int,
        phase: PhaseDef,
        stage: StageDef,
        name: str,
        spark_session_name: str,
        mode: str = "error",
        nlp: bool = False,
        partitioned: bool = True,
        partition_cols: Optional[List] = None,
        row_group_size: int = 128 * 1024 * 1024,  # 128 MB
    ) -> SparkParquetPartitionedDatasetStorageConfig:
        """
        Creates a new `SparkParquetPartitionedDatasetStorageConfig` instance configured for
        partitioned Spark DataFrame storage in parquet format.

        Parameters:
        -----------
        id : int
            The dataset's unique identifier, used for generating file paths.
        phase : PhaseDef
            The current phase (e.g., development, production) used to determine the file path.
        stage : StageDef
            The stage or step within the data pipeline (e.g., extract, transform, load).
        name : str
            The name of the dataset.
        spark_session_name : str
            The name of the Spark session to be used for the dataset.
        mode : str, optional
            The write mode for Spark DataFrame writes (e.g., "error", "overwrite"). Default is "error".
        nlp : bool, optional
            A flag indicating whether the dataset is part of an NLP pipeline. Default is False.
        partitioned : bool, optional
            Whether the dataset should be partitioned. Default is True for this class.
        partition_cols : Optional[List], optional
            A list of columns to use for partitioning the data. Default is ["category"].
        row_group_size : int, optional
            Size of each row group in bytes. Default is 128 MB.

        Returns:
        --------
        SparkParquetPartitionedDatasetStorageConfig
            A new instance of `SparkParquetPartitionedDatasetStorageConfig` with the specified configuration.
        """
        # Create a base file path that includes phase, stage, name, current date, and ID
        filepath = cls.format_filepath(id=id, phase=phase, stage=stage, name=name)

        if partition_cols is None:
            partition_cols = ["category"]

        # Return a SparkParquetPartitionedDatasetStorageConfig with write configurations for Spark
        return cls(
            partitioned=partitioned,
            row_group_size=row_group_size,
            filepath=filepath,
            nlp=nlp,
            spark_session_name=spark_session_name,
            write_kwargs={"partition_cols": partition_cols, "mode": mode},
        )
