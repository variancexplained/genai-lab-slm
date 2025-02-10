#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab                                                                           #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/flow/dataprep/operators/partition.py                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday January 1st 2025 05:54:25 am                                              #
# Modified   : Saturday February 8th 2025 10:43:03 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""Partition Task Module"""

from pyspark.sql import DataFrame

from genailab.flow.base.task import Task
from genailab.infra.service.logging.task import task_logger
from genailab.infra.utils.data.partition import Partitioner


# ------------------------------------------------------------------------------------------------ #
class PartitionTask(Task):
    """
    A task that partitions a PySpark DataFrame.

    This task is responsible for partitioning the input PySpark DataFrame into smaller chunks
    for parallel processing. It calls the `partition_data` function to perform the partitioning.

    Args:
        None

    Methods:
        run(data: DataFrame) -> DataFrame:
            Partitions the input DataFrame and returns the partitioned DataFrame.
    """
    def __init__(self, partitioner: Partitioner) -> None:
        super().__init__()
        self._partitioner = partitioner


    @task_logger
    def run(self, data: DataFrame) -> DataFrame:
        """
        Partitions the input PySpark DataFrame.

        This method takes the input DataFrame, applies the partitioning logic defined in the
        `partition_data` function, and returns the partitioned DataFrame. The method is wrapped
        with the `task_logger` decorator to log task execution details.

        Args:
            data (DataFrame): The input PySpark DataFrame to be partitioned.

        Returns:
            DataFrame: A partitioned PySpark DataFrame, split into smaller chunks for parallel processing.
        """
        data = self._partitioner.partition(data=data)
        self._note = self._partitioner.note
        return data


