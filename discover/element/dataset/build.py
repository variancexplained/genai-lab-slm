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
# Modified   : Wednesday September 25th 2024 11:14:55 pm                                           #
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
from discover.element.dataset.define import Dataset, DatasetMetadata
from discover.element.dataset.store import DatasetStorageConfig
from discover.infra.config.reader import ConfigReader


# ------------------------------------------------------------------------------------------------ #
class DatasetBuilder(ElementBuilder):
    """"""

    def __init__(self, config_reader_cls: type[ConfigReader] = ConfigReader) -> None:
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
        """Sets the name in which the element is called.

        Args:
            name (str): The name associated with the element.

        Returns:
            DatasetBuilder: The builder instance, for chaining.
        """
        self._name = name
        return self

    def phase(self, phase: PhaseDef) -> DatasetBuilder:
        """Sets the phase in which the element is created.

        Args:
            phase (PhaseDef): The phase associated with the element.

        Returns:
            DatasetBuilder: The builder instance, for chaining.
        """
        self._phase = phase
        return self

    def stage(self, stage: StageDef) -> DatasetBuilder:
        """Sets the stage in which the element is created.

        Args:
            stage (StageDef): The stage associated with the element.

        Returns:
            DatasetBuilder: The builder instance, for chaining.
        """
        self._stage = stage
        return self

    def data_structure(self, data_structure: DataStructure) -> DatasetBuilder:
        """Sets data structure.

        Returns:
            DatasetBuilder: The builder instance, for chaining.
        """
        self._data_structure = data_structure
        return self

    def partitioned(self) -> DatasetBuilder:
        """Sets partitioned to True.

        Returns:
            DatasetBuilder: The builder instance, for chaining.
        """
        self._partitioned = True
        return self

    def not_partitioned(self) -> DatasetBuilder:
        """Sets partitioned to False.

        Returns:
            DatasetBuilder: The builder instance, for chaining.
        """
        self._partitioned = False
        return self

    def nlp(self) -> DatasetBuilder:
        """Sets nlp to True if dataset is part of an NLP workflow.

        Returns:
            DatasetBuilder: The builder instance, for chaining.
        """
        self._nlp = True
        return self

    def partition_cols(self, partition_cols: list) -> DatasetBuilder:
        """Sets the columns by which the dataset is to be partitioned

        Also sets partitioned to True

        Args:
            partition_cols: (list): List of variables by which the dataset
                shall be partitioned.

        Returns:
            DatasetBuilder: The builder instance, for chaining.
        """
        self._partitioned = True
        self._partition_cols = partition_cols
        return self

    def content(
        self, content: Union[pd.DataFrame, pyspark.sql.DataFrame]
    ) -> DatasetBuilder:
        """Sets the content of the element.

        Args:
            content (Any): The data or content to be associated with the element.

        Returns:
            DatasetBuilder: The builder instance, for chaining.
        """
        self._content = content
        return self

    def build(self) -> Dataset:
        """Constructs and returns the requested element.

        Returns:
            Dataset: The constructed element instance.
        """
        self._id = self.get_next_id()
        self._validate()  # Validate required fields
        self._configure_metadata()  # Finalize configuration based on required fields
        self._configure_storage()

        # Create Dataset
        dataset = Dataset(
            metadata=self._metadata,
            content=self._content,
            storage_config=self._storage_config,
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
        self._metadata = DatasetMetadata.create(
            id=self._id,
            name=self._name,
            phase=self._phase,
            stage=self._stage,
            nrows=self._nrows,
            ncols=self._ncols,
            size=self._size,
            element_type=self._element_type,
        )

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
        self._read_kwargs = None
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
            "existing_data_behavior": self._dataset_config.pandas.existing_data_behavior,
            "row_group_size": self._row_group_size,
        }
        if self._partitioned:
            self._partition_cols = self._dataset_config.partition_cols
            self._write_kwargs["partition_cols"] = self._partition_cols

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
