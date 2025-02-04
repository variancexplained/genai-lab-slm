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
# Modified   : Monday February 3rd 2025 10:06:32 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
from pyspark.sql import DataFrame


# ------------------------------------------------------------------------------------------------ #
def partition_data(data: DataFrame) -> DataFrame:
    # Estimate the size of the dataset
    dataset_size = data.rdd.map(lambda row: len(str(row))).sum()  # Approximate size (in bytes)

    # Calculate number of partitions based on dataset size
    # Example: For every 1GB of data, use 10 partitions
    partition_size_gb = 1  # 1 GB (can be adjusted based on your system's available memory)
    estimated_partitions = int(dataset_size / (partition_size_gb * 1024**3)) + 1  # Calculate number of partitions

    # Repartition the data
    data_repartitioned = data.repartition(estimated_partitions)

    return data_repartitioned