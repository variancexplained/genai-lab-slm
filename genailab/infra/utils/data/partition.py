#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/infra/utils/data/partition.py                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday February 3rd 2025 10:04:49 pm                                                #
# Modified   : Saturday February 8th 2025 04:54:03 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #

from pyspark.sql import DataFrame


# ------------------------------------------------------------------------------------------------ #
def partition_data(data: DataFrame, target_partition_size_mb: int = 250) -> DataFrame:
    """Repartitions a Spark DataFrame based on an estimated target partition size.

    This function estimates the size of the DataFrame and calculates the number
    of partitions needed to achieve the target partition size. It then
    repartitions the DataFrame using either `coalesce` (if decreasing partitions)
    or `repartition` (if increasing partitions).

    Args:
        data: The input Spark DataFrame.
        target_partition_size_mb: The desired target size for each partition,
            in megabytes. Defaults to 250MB.

    Returns:
        The repartitioned Spark DataFrame.

    """
    current_partitions = data.rdd.getNumPartitions()
    dataset_size = data.rdd.map(lambda row: len(str(row))).sum()
    estimated_partitions = int(dataset_size / (target_partition_size_mb * 1024**2)) + 1

    if estimated_partitions < current_partitions:
        print(f"Decreasing partitions from {current_partitions} to {estimated_partitions}")
        data_repartitioned = data.coalesce(estimated_partitions)
    elif estimated_partitions > current_partitions:
        print(f"Increasing partitions from {current_partitions} to {estimated_partitions}")
        data_repartitioned = data.repartition(estimated_partitions)
    else:
        print(f"Number of partitions remains at {estimated_partitions}")
        data_repartitioned = data

    return data_repartitioned
# ------------------------------------------------------------------------------------------------ #
def partition_data2(data: DataFrame) -> DataFrame:
    # Estimate the size of the dataset
    dataset_size = data.rdd.map(lambda row: len(str(row))).sum()  # Approximate size (in bytes)

    # Calculate number of partitions based on dataset size
    # Example: For every 1GB of data, use 10 partitions
    partition_size_gb = 1  # 1 GB (can be adjusted based on your system's available memory)
    estimated_partitions = int(dataset_size / (partition_size_gb * 1024**3)) + 1  # Calculate number of partitions

    # Repartition the data
    data_repartitioned = data.repartition(estimated_partitions)

    return data_repartitioned