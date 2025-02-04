#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/flow/dataprep/operators/progress.py                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday February 4th 2025 12:07:29 am                                               #
# Modified   : Tuesday February 4th 2025 12:39:21 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""Progress Task Module"""
from pyspark.sql import DataFrame, SparkSession
from tqdm import tqdm

from genailab.flow.base.task import Task
from genailab.infra.service.logging.task import task_logger


# ------------------------------------------------------------------------------------------------ #
class ProgressTask(Task):
    """
    A task to process a PySpark DataFrame with progress tracking.

    This task partitions the input DataFrame, processes it partition by partition, and
    monitors the progress using a progress bar (via tqdm). It leverages Spark's partitioning
    mechanism to perform transformations or other operations on each partition in parallel.

    Args:
        spark (SparkSession): A Spark Session.

    Methods:
        run(data: DataFrame) -> DataFrame:
            Processes the DataFrame partition by partition, tracks progress, and returns
            the processed DataFrame.
    """
    def __init__(self, spark: SparkSession):
        super().__init__()
        self._spark = spark

    @task_logger
    def run(self, data: DataFrame) -> DataFrame:
        """
        Processes the input DataFrame partition by partition and tracks the progress.

        This method uses Spark's partitioning mechanism and processes the data in smaller
        chunks (partitions). The progress of the operation is tracked with a progress bar
        using `tqdm`. After each partition is processed, the progress bar is updated.
        The method returns the processed DataFrame after collecting the results from all partitions.

        Args:
            data (DataFrame): The input PySpark DataFrame to be processed.

        Returns:
            DataFrame: The PySpark DataFrame after processing each partition, collected to the driver.

        Notes:
            - This method uses `rdd.getNumPartitions()` to get the number of partitions and `mapPartitions()`
              to process each partition.
            - The actual computation is triggered when the `collect()` action is called.
            - The method uses the `tqdm` progress bar to show progress for partition processing.
        """

        # Get the schema
        schema = data.schema

        # Get the number of partitions in the dataset
        num_partitions = data.rdd.getNumPartitions()

        # Initialize tqdm progress bar to track partition processing
        with tqdm(total=num_partitions, desc="Processing Partitions") as pbar:
            # Process data partition by partition
            def process_partition(partition):
                # Perform operations on each partition (e.g., transform, filter, etc.)
                return list(partition)  # Convert partition to list (this triggers the actual computation)

            # Use mapPartitions to process the data partition-wise
            rdd_collected = data.rdd.mapPartitions(process_partition).collect()

            # Reconstruct the DataFrame from the RDD using the previously obtained schema
            data_collected = self._spark.createDataFrame(data=rdd_collected, schema=schema)

            # After processing each partition, update the progress bar
            pbar.update(1)  # Update after each partition is processed

        return data_collected

