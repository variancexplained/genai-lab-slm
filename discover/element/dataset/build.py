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
# Modified   : Tuesday September 24th 2024 08:47:21 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from __future__ import annotations

import logging
from abc import abstractmethod
from typing import List, Union

import pandas as pd
import pyspark
from pyspark import StorageLevel

from discover.core.flow import PhaseDef, StageDef
from discover.element.base.build import ElementBuilder
from discover.element.dataset.define import (
    Dataset,
    PandasDataset,
    PandasPartitionedDataset,
    SparkDataset,
    SparkPartitionedDataset,
)
from discover.element.dataset.store import (
    PandasParquetDatasetStorageConfig,
    PandasParquetPartitionedDatasetStorageConfig,
    SparkParquetDatasetStorageConfig,
    SparkParquetPartitionedDatasetStorageConfig,
)
from discover.infra.config.reader import ConfigReader


# ------------------------------------------------------------------------------------------------ #
class DatasetBuilder(ElementBuilder):
    """
    Abstract base class for building an Dataset instance.

    The builder provides methods for configuring various properties of an Dataset
    before constructing it via the build() method.
    """

    def __init__(self, config_reader_cls: type[ConfigReader] = ConfigReader) -> None:
        super().__init__()
        self._config_reader = config_reader_cls()

        # Configuration data
        self._dataset_config = None
        self._spark_config = None
        self._spark_session_name = None
        self._row_group_size = None

        self._id = None
        self._name = "review"
        self._phase = None
        self._stage = None
        self._content = None
        self._partitioned = False
        self._partition_cols = None
        self._storage_config = None

    def reset(self) -> None:
        self._id = None
        self._name = "review"
        self._phase = None
        self._stage = None
        self._content = None
        self._partitioned = False
        self._partition_cols = None
        self._storage_config = None

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

    @abstractmethod
    def build(self) -> Dataset:
        """Constructs and returns the requested element.

        Returns:
            Dataset: The constructed element instance.
        """
        self._validate()

        # Obtain the configs that will be used by builder subclasses.
        self._dataset_config = self._config_reader.get_config(
            section="dataset", namespace=True
        )
        # Spark config contains a mapping between phases and spark sessions contained
        # in the SparkSession pool. It also contains spark session configuration
        # parameters such as memory and retries.
        self._spark_config = self._config_reader.get_config(
            section="spark", namespace=False
        )
        # Obtain the session name for the current phase
        self._spark_session_name = self._spark_config[self._phase.value]

        # Get the row group size for the designated session name.
        self._row_group_size = self._config_reader.get_config(
            section="session", namespace=False
        )[self._spark_session_name]

    def _validate(self) -> None:
        errors = []
        if not self._phase:
            msg = "Phase must be provided."
            errors.append(msg)
        if not self._stage:
            msg = "Stage must be provided."
            errors.append(msg)
        if not self._name:
            msg = "Name must be provided."
            errors.append(msg)
        if self._content is None:
            msg = "Content must be provided."
            errors.append(msg)
        self._log_and_raise(errors=errors)

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


# ------------------------------------------------------------------------------------------------ #
class PandasDatasetBuilder(DatasetBuilder):

    def __init__(self) -> None:
        super().__init__()
        self._partitioned = False
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def build(self) -> PandasDataset:
        super().build()

        id = self.get_next_id()
        self._storage_config = PandasParquetDatasetStorageConfig.create(
            id=id,
            phase=self._phase,
            stage=self._stage,
            name=self._name,
            partitioned=self._partitioned,
            engine=self._dataset_config.pandas.engine,
            compression=self._dataset_config.pandas.compression,
            index=self._dataset_config.pandas.index,
            row_group_size=self._row_group_size,
        )

        dataset = PandasDataset(
            id=id,
            name=self._name,
            phase=self._phase,
            stage=self._stage,
            storage_config=self._storage_config,
            content=self._content,
            nrows=self._content.shape[0],
            ncols=self._content.shape[1],
            size=self._content.memory_usage(deep=True).sum(),
        )

        self.reset()

        return dataset


# ------------------------------------------------------------------------------------------------ #
class PandasPartitionedDatasetBuilder(DatasetBuilder):

    def __init__(self) -> None:
        super().__init__()
        self._partitioned = True
        self._partition_cols = None
        self._existing_data_behavior = "error"
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def reset(self) -> None:
        super().reset()
        self._partitioned = True
        self._partition_cols = None
        self._existing_data_behavior = "error"

    def partition_cols(self, partition_cols: list) -> PandasPartitionedDatasetBuilder:
        self._partition_cols = partition_cols
        return self

    def existing_data_behavior(
        self, existing_data_behavior
    ) -> PandasPartitionedDatasetBuilder:
        self._existing_data_behavior = existing_data_behavior
        return self

    def build(self) -> PandasPartitionedDataset:
        super().build()
        id = self.get_next_id()

        self._partition_cols = self._dataset_config.pandas.partition_cols
        self._existing_data_behavior = (
            self._dataset_config.pandas.existing_data_behavior
        )

        self._storage_config = PandasParquetPartitionedDatasetStorageConfig.create(
            id=id,
            phase=self._phase,
            stage=self._stage,
            name=self._name,
            partitioned=self._partitioned,
            engine=self._dataset_config.pandas.engine,
            compression=self._dataset_config.pandas.compression,
            index=self._dataset_config.pandas.index,
            partition_cols=self._partition_cols,
            existing_data_behavior=self._existing_data_behavior,
            row_group_size=self._row_group_size,
        )

        dataset = PandasPartitionedDataset(
            id=id,
            name=self._name,
            phase=self._phase,
            stage=self._stage,
            storage_config=self._storage_config,
            content=self._content,
            nrows=self._content.shape[0],
            ncols=self._content.shape[1],
            size=self._content.memory_usage(deep=True).sum(),
        )

        self.reset()

        return dataset


# ------------------------------------------------------------------------------------------------ #
class SparkDatasetBuilder(DatasetBuilder):

    def __init__(self) -> None:
        super().__init__()
        self._partitioned = False
        self._mode = "error"
        self._nlp = False

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def reset(self) -> None:
        super().reset()
        self._mode = "error"
        self._nlp = False

    def mode(self, mode: str) -> SparkDatasetBuilder:
        self._mode = mode
        return self

    def nlp(self) -> SparkDatasetBuilder:
        self._nlp = True
        return self

    def build(self) -> SparkDataset:

        super().build()

        self._mode = self._mode or self._dataset_config.spark.mode

        id = self.get_next_id()
        self._storage_config = SparkParquetDatasetStorageConfig.create(
            id=id,
            phase=self._phase,
            stage=self._stage,
            name=self._name,
            nlp=self._nlp,
            mode=self._mode,
            partitioned=self._partitioned,
            row_group_size=self._row_group_size,
            spark_session_name=self._spark_session_name,
        )

        # Obtain memory footprint
        # Cache the DataFrame in memory
        self._content.persist(StorageLevel.MEMORY_ONLY)

        # Calculate the size in bytes of each partition and sum them
        total_size_in_bytes = self._content.rdd.map(lambda row: len(str(row))).sum()

        dataset = SparkDataset(
            id=id,
            name=self._name,
            phase=self._phase,
            stage=self._stage,
            storage_config=self._storage_config,
            content=self._content,
            nrows=self._content.count(),
            ncols=len(self._content.columns),
            size=total_size_in_bytes,
        )

        self.reset()

        return dataset


# ------------------------------------------------------------------------------------------------ #
class SparkPartitionedDatasetBuilder(DatasetBuilder):

    def __init__(self) -> None:
        super().__init__()
        self._partitioned = True
        self._mode = "error"
        self._nlp = False

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def reset(self) -> None:
        super().reset()
        self._mode = "error"
        self._nlp = False

    def mode(self, mode: str) -> SparkPartitionedDatasetBuilder:
        self._mode = mode
        return self

    def nlp(self) -> SparkPartitionedDatasetBuilder:
        self._nlp = True
        return self

    def partition_cols(self, partition_cols: list) -> PandasPartitionedDatasetBuilder:
        self._partition_cols = partition_cols
        return self

    def build(self) -> SparkPartitionedDataset:

        super().build()

        self._mode = self._mode or self._dataset_config.spark.mode
        self._partition_cols = (
            self._partition_cols or self._dataset_config.spark.partition_cols
        )

        id = self.get_next_id()
        self._storage_config = SparkParquetPartitionedDatasetStorageConfig.create(
            id=id,
            phase=self._phase,
            stage=self._stage,
            name=self._name,
            nlp=self._nlp,
            partitioned=self._partitioned,
            mode=self._mode,
            partition_cols=self._partition_cols,
            row_group_size=self._row_group_size,
            spark_session_name=self._spark_session_name,
        )
        # Obtain memory footprint
        # Cache the DataFrame in memory
        self._content.persist(StorageLevel.MEMORY_ONLY)

        # Calculate the size in bytes of each partition and sum them
        total_size_in_bytes = self._content.rdd.map(lambda row: len(str(row))).sum()

        dataset = SparkPartitionedDataset(
            id=id,
            name=self._name,
            phase=self._phase,
            stage=self._stage,
            storage_config=self._storage_config,
            content=self._content,
            nrows=self._content.count(),
            ncols=len(self._content.columns),
            size=total_size_in_bytes,
        )

        self.reset()

        return dataset
