#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab                                                                           #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/infra/utils/data/partition.py                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday February 3rd 2025 10:04:49 pm                                                #
# Modified   : Saturday February 8th 2025 10:43:30 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Union

import dask.dataframe as dd
import pandas as pd
from pandas import DataFrame
from pyspark.sql import DataFrame

from genailab.core.dstruct import DataClass


# ------------------------------------------------------------------------------------------------ #
@dataclass
class PartitionConfig(DataClass):
    npartitions: int
    chunksize: int


# ------------------------------------------------------------------------------------------------ #
class Partitioner(ABC):
    """Abstract base class for data partitioners."""

    @property
    def note(self) -> str:
        return self._note

    @abstractmethod
    def partition(self, data: Union[DataFrame, dd.DataFrame]) -> Union[DataFrame, dd.DataFrame]:
        """Partitions the input data.

        Args:
            data (Union[DataFrame, dd.DataFrame]): The data to partition (Pandas or Dask DataFrame).

        Returns:
            Union[DataFrame, dd.DataFrame]: The partitioned data (Pandas or Dask DataFrame).
        """
        pass  # Abstract method, must be implemented by subclasses

# ------------------------------------------------------------------------------------------------ #
#                                       SPARK PARTITIONER                                          #
# ------------------------------------------------------------------------------------------------ #
class SparkPartitioner(Partitioner):
    """Partitions data using Spark's repartitioning capabilities.

    Attributes:
        _target_partition_size (int): The target partition size in bytes.

    Args:
        target_partition_size (int, optional): The target partition size in bytes. Defaults to 256MB.
    """

    def __init__(self, target_partition_size: int = 268435456) -> None:
        self._target_partition_size = target_partition_size
        self._note = None

    def partition(self, data: DataFrame) -> DataFrame:
        """Partitions the Spark DataFrame based on estimated size.

        Args:
            data (DataFrame): The Spark DataFrame to partition.

        Returns:
            DataFrame: The repartitioned Spark DataFrame.
        """
        current_partitions = data.rdd.getNumPartitions()
        dataset_size = data.rdd.map(lambda row: len(str(row))).sum()
        estimated_partitions = int(dataset_size / self._target_partition_size) + 1

        if estimated_partitions < current_partitions:
            self._note = f"Decreased partitions from {current_partitions} to {estimated_partitions}"
            data_repartitioned = data.coalesce(estimated_partitions)
        elif estimated_partitions > current_partitions:
            self._note = f"Increased partitions from {current_partitions} to {estimated_partitions}"
            data_repartitioned = data.repartition(estimated_partitions)
        else:
            self._note = f"Number of partitions remained at {estimated_partitions}"
            data_repartitioned = data

        return data_repartitioned
# ------------------------------------------------------------------------------------------------ #
#                                       DASK PARTITIONER                                           #
# ------------------------------------------------------------------------------------------------ #
class DaskPartitioner(Partitioner):
    """Partitions data using Dask's repartitioning capabilities.

    Attributes:
        _num_cores (int): The number of cores to use.
        _worker_memory (int): The memory available per worker in bytes.
        _target_partition_size (int): The target partition size in bytes.
        _min_partitions (int): The minimum number of partitions.

    Args:
        num_cores (int, optional): The number of cores to use. Defaults to 18 or os.cpu_count().
        worker_memory (int, optional): The memory available per worker in bytes. Defaults to 6GB.
        target_partition_size (int, optional): The target partition size in bytes. Defaults to 256MB.
        min_partitions (int, optional): The minimum number of partitions. Defaults to 4.
    """

    def __init__(self, num_cores: int = 18, worker_memory: int = 6442450944, target_partition_size: int = 268435456, min_partitions: int = 4) -> None:
        self._num_cores = num_cores or os.cpu_count()
        self._worker_memory = worker_memory
        self._target_partition_size = target_partition_size
        self._min_partitions = min_partitions


    def optimize_pandas_to_dask(self, df: pd.DataFrame) -> PartitionConfig:
        data_size = df.memory_usage().sum()
        npartitions = self.optimal_partitions(data_size=data_size)
        chunksize = int(data_size/npartitions)
        return PartitionConfig(npartitions=npartitions, chunksize=chunksize)

    def optimal_partitions(self, data_size: int) -> int:
        """Calculates the optimal number of partitions for Dask.

        Args:
            data_size (int): The size of the data in bytes.

        Returns:
            int: The optimal number of partitions.
        """
        ideal_partitions = max(self._min_partitions, int(data_size / self._target_partition_size))
        partitions = min(ideal_partitions, self._num_cores * 4, int(data_size / self._worker_memory)+1)
        return int(partitions)

    def partition(self, ddf: dd.DataFrame) -> dd.DataFrame:
        """Partitions the Dask DataFrame based on estimated size and available resources.

        Args:
            ddf (dd.DataFrame): The Dask DataFrame to partition.

        Returns:
            dd.DataFrame: The repartitioned Dask DataFrame.

        Raises:
            ValueError: If the data size cannot be inferred.
        """
        try:
            data_size = ddf.memory_usage().sum().compute()
        except Exception as e:
            raise ValueError(f"Could not infer data size: {e}. Please provide it explicitly.")

        n_partitions = self.optimal_partitions(data_size=data_size)
        return ddf.repartition(npartitions=n_partitions)