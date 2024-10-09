#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/element/dataset/build.py                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 01:35:11 am                                              #
# Modified   : Tuesday October 8th 2024 09:22:02 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from __future__ import annotations

import logging
from typing import List, Union

import pandas as pd
import pyspark

from discover.core.data_structure import DataStructure
from discover.core.flow import PhaseDef, StageDef
from discover.element.base.build import ElementBuilder
from discover.element.dataset.define import Dataset
from discover.element.dataset.store import DatasetStorageConfig
from discover.infra.config.reader import ConfigReader


# ------------------------------------------------------------------------------------------------ #
class DatasetBuilder(ElementBuilder):
    """
    A builder class responsible for constructing `Dataset` objects. The `DatasetBuilder` class
    provides an interface for configuring various aspects of a dataset, including its content,
    storage configuration (e.g., pandas or Spark), partitioning, and metadata. It reads configuration
    settings and manages various options like partitioning, compression, and engine settings for
    both Pandas and Spark-based datasets.

    Attributes:
        _config_reader (ConfigReader): An instance of the ConfigReader class to load configuration settings.
        _logger (logging.Logger): Logger for the class.
        _dataset_config (dict): Configuration settings specific to the dataset section.
        _spark_config (dict): Configuration settings specific to the Spark section.
        _id (str or None): The identifier for the dataset.
        _name (str): The name of the dataset.
        _phase (PhaseDef or None): The phase associated with the dataset (e.g., for different stages of a project).
        _stage (StageDef or None): The stage of the dataset creation process.
        _content (pd.DataFrame or pyspark.sql.DataFrame or None): The actual dataset content.
        _data_structure (DataStructure): Specifies whether the dataset uses Pandas or Spark.
        _element_type (str): Type of element being built, set to Dataset.
        _partitioned (bool): Indicates if the dataset is partitioned.
        _partition_cols (list or None): Columns by which the dataset is partitioned.
        _engine (str or None): The engine used for reading/writing (Pandas-specific).
        _compression (str or None): Compression method used during storage (Pandas-specific).
        _index (bool or None): Index behavior for the dataset (Pandas-specific).
        _existing_data_behavior (str or None): Behavior when existing data is encountered (Pandas-specific).
        _row_group_size (int or None): Size of row groups, dependent on the phase.
        _spark_session_name (str or None): The Spark session name used in Spark-based datasets.
        _mode (str or None): Mode for Spark-based datasets (e.g., overwrite, append).
        _nlp (bool): Indicates if the dataset is part of an NLP workflow.
        _parquet_block_size (int or None): Parquet block size for Spark-based datasets.
        _read_kwargs (dict): Keyword arguments for reading the dataset.
        _write_kwargs (dict): Keyword arguments for writing the dataset.
        _nrows (int): Number of rows in the dataset.
        _ncols (int): Number of columns in the dataset.
        _size (float): Size of the dataset in bytes.
    """

    def __init__(self, config_reader_cls: type[ConfigReader] = ConfigReader) -> None:
        """
        Initializes a new instance of the `DatasetBuilder` class.

        Args:
            config_reader_cls (type[ConfigReader], optional): Class type used to load configuration settings.
                Defaults to ConfigReader.
        """
        super().__init__()
        self._config_reader = config_reader_cls()
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        # Configuration data
        self._dataset_config = self._config_reader.get_config(section="dataset")
        self._spark_config = self._config_reader.get_config(
            section="spark", namespace=False
        )
        self._id = None
        self._name = "review"
        self._phase = None
        self._stage = None
        self._content = None
        # Common storage related members and default values
        self._data_structure = DataStructure.PANDAS
        self._element_type = Dataset.__name__
        self._partitioned = False
        self._partition_cols = None
        # Pandas specific storage related members and default values
        self._engine = None
        self._compression = None
        self._index = None
        self._existing_data_behavior = None
        self._row_group_size = None
        # Spark specific storage related members and default values
        self._mode = None
        # Values that depend upon other member values set during the build process.
        # These values will be set in the configure method called during the build process
        self._spark_session_name = None  # Depends on the phase
        self._row_group_size = None  # Depends on phase
        self._storage_config = (
            None  # Depends on pandas vs spark, and partitioned vs non-partitioned.
        )
        self._nlp = False
        self._parquet_block_size = None
        # IO Kwargs
        self._read_kwargs = None
        self._write_kwargs = None
        # The following are content related metadata
        self._nrows: int = 0
        self._ncols: int = 0
        self._size: float = 0

    def reset(self) -> None:
        """
        Resets all attributes of the builder to their default state. This is typically called
        after a dataset has been built to prepare the builder for constructing a new dataset.
        """
        self._id = None
        self._name = "review"
        self._phase = None
        self._stage = None
        self._content = None
        # Common storage related members and default values
        self._data_structure = None
        self._element_type = Dataset.__name__
        self._partitioned = False
        self._partition_cols = None
        # Pandas specific storage related members and default values
        self._engine = None
        self._compression = None
        self._index = None
        self._existing_data_behavior = None
        self._row_group_size = None
        # Spark specific storage related members and default values
        self._mode = None
        # Values that depend upon other member values set during the build process.
        # These values will be set in the configure method called during the build process
        self._spark_session_name = None  # Depends on the phase
        self._row_group_size = None  # Depends on phase
        self._storage_config = (
            None  # Depends on pandas vs spark, and partitioned vs non-partitioned.
        )
        self._nlp = False
        self._parquet_block_size = None
        # IO Kwargs
        self._read_kwargs = None
        self._write_kwargs = None
        # The following are content related metadata
        self._nrows: int = 0
        self._ncols: int = 0
        self._size: float = 0

    def name(self, name: str) -> DatasetBuilder:
        """
        Sets the name of the dataset.

        Args:
            name (str): The name associated with the dataset.

        Returns:
            DatasetBuilder: The builder instance, for chaining.
        """
        self._name = name
        return self

    def phase(self, phase: PhaseDef) -> DatasetBuilder:
        """
        Sets the phase of the dataset creation process.

        Args:
            phase (PhaseDef): The phase (e.g., training, testing) of the dataset creation.

        Returns:
            DatasetBuilder: The builder instance, for chaining.
        """
        self._phase = phase
        return self

    def stage(self, stage: StageDef) -> DatasetBuilder:
        """
        Sets the stage in which the dataset is being created.

        Args:
            stage (StageDef): The stage associated with the dataset.

        Returns:
            DatasetBuilder: The builder instance, for chaining.
        """
        self._stage = stage
        return self

    def data_structure(self, data_structure: DataStructure) -> DatasetBuilder:
        """
        Sets the data structure type (e.g., Pandas or Spark) for the dataset.

        Args:
            data_structure (DataStructure): The data structure to be used.

        Returns:
            DatasetBuilder: The builder instance, for chaining.
        """
        self._data_structure = data_structure
        return self

    def partitioned(self) -> DatasetBuilder:
        """
        Marks the dataset as partitioned.

        Returns:
            DatasetBuilder: The builder instance, for chaining.
        """
        self._partitioned = True
        return self

    def not_partitioned(self) -> DatasetBuilder:
        """
        Marks the dataset as not partitioned.

        Returns:
            DatasetBuilder: The builder instance, for chaining.
        """
        self._partitioned = False
        return self

    def nlp(self) -> DatasetBuilder:
        """
        Sets the dataset as part of an NLP workflow.

        Returns:
            DatasetBuilder: The builder instance, for chaining.
        """
        self._nlp = True
        return self

    def partition_cols(self, partition_cols: list) -> DatasetBuilder:
        """
        Sets the partition columns and marks the dataset as partitioned.

        Args:
            partition_cols (list): List of columns by which the dataset will be partitioned.

        Returns:
            DatasetBuilder: The builder instance, for chaining.
        """
        self._partitioned = True
        self._partition_cols = partition_cols
        return self

    def content(
        self, content: Union[pd.DataFrame, pyspark.sql.DataFrame]
    ) -> DatasetBuilder:
        """
        Sets the content of the dataset.

        Args:
            content (Union[pd.DataFrame, pyspark.sql.DataFrame]): The dataset content.

        Returns:
            DatasetBuilder: The builder instance, for chaining.
        """
        self._content = content
        return self

    def build(self) -> Dataset:
        """
        Finalizes the configuration and constructs the `Dataset` object.

        Returns:
            Dataset: The constructed dataset.
        """
        self._id = self.get_next_id()
        self._validate()  # Validate required fields
        self._configure_metadata()  # Extract metadata from content
        self._configure_storage()  # Configure storage for content

        # Create Dataset
        dataset = Dataset(
            id=self._id,
            name=self._name,
            phase=self._phase,
            stage=self._stage,
            content=self._content,
            storage_config=self._storage_config,
            nrows=self._nrows,
            ncols=self._ncols,
            size=self._size,
        )

        # Reset the builder
        self.reset()

        return dataset

    def _validate(self) -> None:
        errors = []
        if not self._id:
            msg = "Id has not been set."
            errors.append(msg)
        if not self._phase:
            msg = "Phase must be provided."
            errors.append(msg)
        if not isinstance(self._data_structure, DataStructure):
            msg = f"Expected a DataStructure instance for data_structure. Encountered {type(self._data_structure).__name__}."
        if not isinstance(self._phase, PhaseDef):
            msg = f"Expected a PhaseDef instance for phase. Encountered {type(self._phase).__name__}."
            errors.append(msg)
        if not self._stage:
            msg = "Stage must be provided."
            errors.append(msg)
        if not isinstance(self._stage, StageDef):
            msg = f"Expected a StageDef instance for phase. Encountered {type(self._phase).__name__}."
            errors.append(msg)
        if not self._name:
            msg = "Name must be provided."
            errors.append(msg)
        if self._content is None:
            msg = "Content must be provided."
            errors.append(msg)

        self._log_and_raise(errors=errors)

    def _configure_metadata(self) -> None:
        if self._data_structure == DataStructure.SPARK:
            self._nrows = self._content.count()
            self._ncols = len(self._content.columns)
            self._size = self._content.rdd.map(lambda row: len(str(row))).sum()
        else:
            self._nrows = self._content.shape[0]
            self._ncols = self._content.shape[1]
            self._size = self._content.memory_usage(deep=True).sum()

    def _configure_storage(self) -> None:
        """Finalizes the builder prior to constructing the dataset"""
        if self._data_structure == DataStructure.SPARK:
            self._configure_spark_dataset_storage()
        else:
            self._configure_pandas_dataset_storage()

    def _configure_spark_dataset_storage(self) -> None:
        # Set data structure
        self._data_structure = DataStructure.SPARK
        # The spark session to be used for io
        self._spark_session_name = self._spark_config[self._phase.value]
        # Mode specifies behavior when writing spark dataframes vis-a-vis existing data
        self._mode = self._dataset_config.spark.mode
        # Parquet group size is analogous to row group size and is set on the spark session
        parquet_block_sizes = self._config_reader.get_config(
            section="dataset", namespace=False
        )["spark"]["parquet_block_size"]
        self._parquet_block_size = parquet_block_sizes[self._spark_session_name]
        # Format read and write kwargs arguments
        self._read_kwargs = {}
        self._write_kwargs = {"mode": self._mode}
        if self._partitioned:
            self._partition_cols = self._dataset_config.partition_cols
            self._write_kwargs["partitionBy"] = self._partition_cols

        self._storage_config = DatasetStorageConfig.create(
            id=self._id,
            phase=self._phase,
            stage=self._stage,
            name=self._name,
            data_structure=self._data_structure,
            spark_session_name=self._spark_session_name,
            nlp=self._nlp,
            mode=self._mode,
            parquet_block_size=self._parquet_block_size,
            read_kwargs=self._read_kwargs,
            write_kwargs=self._write_kwargs,
            partitioned=self._partitioned,
            partition_cols=self._partition_cols,
        )

    def _configure_pandas_dataset_storage(self) -> None:
        # Set data structure
        self._data_structure = DataStructure.PANDAS
        # Set in configuration file
        self._engine = self._dataset_config.pandas.engine
        self._compression = self._dataset_config.pandas.compression
        self._index = self._dataset_config.pandas.index
        self._existing_data_behavior = (
            self._dataset_config.pandas.existing_data_behavior
        )
        # Row group size is phase dependent
        row_group_sizes = self._config_reader.get_config(
            section="dataset", namespace=False
        )["pandas"]["row_group_size"]
        self._row_group_size = row_group_sizes[self._phase.value]
        # Format the read and write kwargs
        self._read_kwargs = {"engine": self._engine}
        self._write_kwargs = {
            "engine": self._engine,
            "compression": self._dataset_config.pandas.compression,
            "index": self._dataset_config.pandas.index,
            "row_group_size": self._row_group_size,
        }
        if self._partitioned:
            self._partition_cols = self._dataset_config.partition_cols
            self._write_kwargs["partition_cols"] = self._partition_cols
            self._write_kwargs["existing_data_behavior"] = (
                self._dataset_config.pandas.existing_data_behavior
            )

        self._storage_config = DatasetStorageConfig.create(
            id=self._id,
            phase=self._phase,
            stage=self._stage,
            name=self._name,
            data_structure=self._data_structure,
            engine=self._engine,
            compression=self._compression,
            index=self._index,
            existing_data_behavior=self._existing_data_behavior,
            row_group_size=self._row_group_size,
            read_kwargs=self._read_kwargs,
            write_kwargs=self._write_kwargs,
            partitioned=self._partitioned,
            partition_cols=self._partition_cols,
        )

    def _log_and_raise(self, errors: List[str]) -> None:
        """
        Logs the provided list of errors and raises an `InvalidConfigException` if errors are present.

        Parameters:
        -----------
        errors : List[str]
            A list of validation error messages.
        """
        if errors:
            error_msg = "\n".join(errors)
            self._logger.error(error_msg)
            raise RuntimeError(error_msg)
