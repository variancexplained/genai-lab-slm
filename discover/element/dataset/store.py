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
# Modified   : Monday September 23rd 2024 07:27:15 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from __future__ import annotations

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
    A data class that encapsulates storage-related information for datasets. This includes
    details about the data structure (e.g., Pandas or PySpark), file partitioning, and
    any read/write arguments required for storing and retrieving datasets.

    The file path generation has been centralized in the `format_filepath` class method,
    which creates a standardized file path based on the provided phase, stage, name, and
    current date.

    Attributes:
        structure (DataStructure): Defines the data structure type for the dataset, with
            Pandas as the default. Other options might include PySpark or other data structures.
        partitioned (bool): Indicates whether the dataset is partitioned. Defaults to True.
        filepath (str): The generated file path and name for the dataset, optionally
            partitioned or non-partitioned based on configuration.
        read_kwargs (Dict[str, Any]): A dictionary of keyword arguments for reading the dataset.
        write_kwargs (Dict[str, Any]): A dictionary of keyword arguments for writing the dataset.

    Methods:
        format_filepath(cls, id, phase, stage, name):
            Generates a standardized file path based on the phase, stage, name, and current
            date, along with a zero-padded ID to ensure uniqueness. This method centralizes
            the logic for generating file paths across different storage configurations.
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
        Generates a file path based on the provided phase, stage, name, current date (in YYYYMMDD format),
        and a zero-padded ID to ensure uniqueness. This method centralizes the logic for generating file
        paths, ensuring consistency across different dataset configurations.

        Args:
            id (int): A unique identifier for the dataset, used to ensure the filename is unique.
            phase (PhaseDef): The phase of the dataset's lifecycle or process.
            stage (StageDef): The stage within the phase that this dataset corresponds to.
            name (str): A descriptive name for the dataset.

        Returns:
            str: The generated file path, which can be used to store or retrieve the dataset.
        """
        return f"{phase.value}_{stage.value}_{name}_dataset_{datetime.now().strftime('%Y%m%d')}-{str(id).zfill(4)}"

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
        A factory method to create a DatasetStorageConfig object, generating a file path based
        on the provided phase, stage, name, and the current date. The generated file path can
        be partitioned or non-partitioned depending on the configuration.

        The filename includes the phase, stage, name, current date (in YYYYMMDD format), and a
        zero-padded ID to ensure uniqueness. If partitioning is disabled, the `.parquet` extension
        is appended to the file name.

        Args:
            id (int): A unique identifier for the dataset, used to ensure the filename is unique.
            phase (PhaseDef): The phase of the dataset's lifecycle or process.
            stage (StageDef): The stage within the phase that this dataset corresponds to.
            name (str): A descriptive name for the dataset.
            partitioned (bool, optional): A flag indicating whether the dataset is partitioned.
                Defaults to True.

        Returns:
            DatasetStorageConfig: A new instance of DatasetStorageConfig with a generated file path.
        """
        # Create a base file path that includes phase, stage, name, current date, and ID
        filepath = cls.format_filepath(id=id, phase=phase, stage=stage, name=name)

        # If not partitioned, append the .parquet extension
        filepath = filepath + ".parquet" if not partitioned else filepath

        # Return a new DatasetStorageConfig instance with the generated filepath and other settings
        return cls(partitioned=partitioned, filepath=filepath)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class PandasParquetDatasetStorageConfig(DatasetStorageConfig):
    """"""

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
        index: bool = False,
        row_group_size: int = 128 * 1024 * 1024,  # 128 MB
    ) -> PandasParquetDatasetStorageConfig:
        """"""
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
    A specialized data class for handling storage-related information specific to datasets
    in Pandas DataFrame format. This class extends DatasetStorageConfig and provides additional
    support for partitioning and writing configurations tailored for Pandas.

    Attributes (inherited from DatasetStorageConfig):
        structure (DataStructure): Defines the data structure type, set to Pandas by default.
        partitioned (bool): Indicates whether the dataset is partitioned. Defaults to True.
        filepath (str): The generated file path and name for the dataset.
        read_kwargs (Dict[str, Any]): A dictionary of keyword arguments for reading the dataset.
        write_kwargs (Dict[str, Any]): A dictionary of keyword arguments for writing the dataset.

    Methods:
        create(cls, id, phase, stage, name, partitioned=True, partition_cols=["category"]):
            A factory method that creates a PandasParquetDatasetStorageConfig object, generating
            a file path based on the provided phase, stage, name, and the current date, and
            including partitioning options and Pandas-specific writing configurations.
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
        index: bool = False,
        row_group_size: int = 128 * 1024 * 1024,  # 128 MB
        partition_cols: Optional[List] = None,
        existing_data_behavior: str = "delete_matching",
    ) -> PandasParquetPartitionedDatasetStorageConfig:
        """"""
        # Create a base file path that includes phase, stage, name, current date, and ID
        filepath = cls.format_filepath(id=id, phase=phase, stage=stage, name=name)

        # Handle mutable default
        if partition_cols is None:
            partition_cols = ["category"]

        # Return a PandasParquetDatasetStorageConfig with write configurations for Pandas
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
    """ """

    structure: DataStructure = DataStructure.SPARK  # Set the default structure to Spark

    @classmethod
    def create(
        cls,
        id: int,
        phase: PhaseDef,
        stage: StageDef,
        name: str,
        mode: str = "error",
        partitioned: bool = False,
        row_group_size: int = 128 * 1024 * 1024,  # 128 MB
    ) -> SparkParquetDatasetStorageConfig:
        """
        A factory method to create a SparkParquetDatasetStorageConfig object, generating a file path
        based on the provided phase, stage, name, and the current date. It also configures
        Spark-specific write options, including partitioning by columns if provided.

        The filename includes the phase, stage, name, current date (in YYYYMMDD format), and a
        zero-padded ID to ensure uniqueness. If partitioning is disabled, the `.parquet` extension
        is appended to the file name. If partition columns are provided, the write configuration
        includes the specified columns for partitioning.

        Args:
            id (int): A unique identifier for the dataset, used to ensure the filename is unique.
            phase (PhaseDef): The phase of the dataset's lifecycle or process.
            stage (StageDef): The stage within the phase that this dataset corresponds to.
            name (str): A descriptive name for the dataset.
            partitioned (bool, optional): A flag indicating whether the dataset is partitioned.
                Defaults to True.
            partition_cols (list, optional): A list of column names to partition the dataset by.
                Defaults to ["category"].

        Returns:
            SparkParquetDatasetStorageConfig: A new instance of SparkParquetDatasetStorageConfig with the appropriate
            file path and Spark-specific write configurations.
        """
        # Create a base file path that includes phase, stage, name, current date, and ID
        filepath = (
            cls.format_filepath(id=id, phase=phase, stage=stage, name=name) + ".parquet"
        )

        # Return a SparkParquetDatasetStorageConfig with write configurations for Spark
        return cls(
            partitioned=partitioned,
            filepath=filepath,
            row_group_size=row_group_size,
            write_kwargs={"mode": mode},
        )


# ------------------------------------------------------------------------------------------------ #
@dataclass
class SparkParquetPartitionedDatasetStorageConfig(DatasetStorageConfig):
    """
    A specialized data class for handling storage-related information specific to datasets
    in PySpark DataFrame format. This class extends DatasetStorageConfig and provides additional
    support for partitioning and configurations tailored for Spark.

    Attributes (inherited from DatasetStorageConfig):
        structure (DataStructure): Defines the data structure type, set to Spark by default.
        partitioned (bool): Indicates whether the dataset is partitioned. Defaults to True.
        filepath (str): The generated file path and name for the dataset.
        read_kwargs (Dict[str, Any]): A dictionary of keyword arguments for reading the dataset.
        write_kwargs (Dict[str, Any]): A dictionary of keyword arguments for writing the dataset.

    Methods:
        create(cls, id, phase, stage, name, partitioned=True, partition_cols=["category"]):
            A factory method that creates a SparkParquetDatasetStorageConfig object, generating
            a file path based on the provided phase, stage, name, and the current date, and
            including partitioning options for Spark.
    """

    structure: DataStructure = DataStructure.SPARK  # Set the default structure to Spark

    @classmethod
    def create(
        cls,
        id: int,
        phase: PhaseDef,
        stage: StageDef,
        name: str,
        mode: str = "error",
        partitioned: bool = True,
        partition_cols: Optional[List] = None,
        row_group_size: int = 128 * 1024 * 1024,  # 128 MB
    ) -> SparkParquetPartitionedDatasetStorageConfig:
        """
        A factory method to create a SparkParquetDatasetStorageConfig object, generating a file path
        based on the provided phase, stage, name, and the current date. It also configures
        Spark-specific write options, including partitioning by columns if provided.

        The filename includes the phase, stage, name, current date (in YYYYMMDD format), and a
        zero-padded ID to ensure uniqueness. If partitioning is disabled, the `.parquet` extension
        is appended to the file name. If partition columns are provided, the write configuration
        includes the specified columns for partitioning.

        Args:
            id (int): A unique identifier for the dataset, used to ensure the filename is unique.
            phase (PhaseDef): The phase of the dataset's lifecycle or process.
            stage (StageDef): The stage within the phase that this dataset corresponds to.
            name (str): A descriptive name for the dataset.
            partitioned (bool, optional): A flag indicating whether the dataset is partitioned.
                Defaults to True.
            partition_cols (list, optional): A list of column names to partition the dataset by.
                Defaults to ["category"].

        Returns:
            SparkParquetDatasetStorageConfig: A new instance of SparkParquetDatasetStorageConfig with the appropriate
            file path and Spark-specific write configurations.
        """
        # Create a base file path that includes phase, stage, name, current date, and ID
        filepath = cls.format_filepath(id=id, phase=phase, stage=stage, name=name)

        if partition_cols is None:
            partition_cols = ["category"]

        # Return a SparkParquetDatasetStorageConfig with write configurations for Spark

        return cls(
            partitioned=partitioned,
            row_group_size=row_group_size,
            filepath=filepath,
            write_kwargs={"partition_cols": partition_cols, "mode": mode},
        )
