#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/persistence/repo/config.py                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday October 10th 2024 04:58:59 pm                                              #
# Modified   : Thursday December 19th 2024 04:48:41 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Repository Config Module"""
from dataclasses import field
from typing import Optional

from pydantic.dataclasses import dataclass

from discover.assets.data import StorageConfig
from discover.infra.config.app import AppConfigReader

# ------------------------------------------------------------------------------------------------ #
config_reader = AppConfigReader()
# ------------------------------------------------------------------------------------------------ #


@dataclass
class CentralizedDatasetStorageConfig(StorageConfig):
    """
    Configuration class for centralized dataset storage, specifically for managing
    Parquet files with partitioning and compression options.

    Attributes:
        partitioned (bool): Determines if the dataset should be partitioned by columns.
            Defaults to True.
        partition_cols (list): List of column names to use for partitioning the dataset.
            Defaults to an empty list, but is set to ["category"] if partitioned is True.
        engine (str): The engine to use for reading and writing Parquet files.
            Defaults to "pyarrow".
        compression (str): The compression algorithm for Parquet files.
            Defaults to "snappy".
        index (bool): Whether to include the index when writing the Parquet file.
            Defaults to True.
        existing_data_behavior (str): Defines the behavior when existing data is found.
            Options could include strategies like "delete_caching", "overwrite", etc.
            Defaults to "delete_caching".
        row_group_size (Optional[int]): Defines the row group size for the Parquet file,
            which can influence performance. Defaults to None, and is later set from
            the configuration file.

    Methods:
        __post_init__() -> None:
            Initializes additional configurations after the object is created. It retrieves
            settings from the configuration reader, adjusts row group size, and sets up
            keyword arguments for reading and writing Parquet files.
    """

    partitioned: bool = True
    partition_cols: list = field(default_factory=list)
    engine: str = "pyarrow"
    compression: str = "snappy"
    index: bool = True
    existing_data_behavior: str = "delete_matching"
    row_group_size: Optional[int] = None

    def __post_init__(self) -> None:
        """
        Initializes the storage configuration with additional parameters after object creation.
        Retrieves Parquet-specific settings from a configuration file and prepares keyword
        arguments for read and write operations based on the provided settings.

        Raises:
            KeyError: If the "file" section or required settings are missing in the configuration file.
        """
        file_config = config_reader.get_config(section="file")

        self.row_group_size = self.row_group_size or file_config.parquet.block_size

        self.read_kwargs = {"engine": self.engine}
        self.write_kwargs = {
            "engine": self.engine,
            "compression": self.compression,
            "index": self.index,
            "row_group_size": self.row_group_size,
        }
        if self.partitioned:
            self.partition_cols = ["category"]
            self.write_kwargs["partition_cols"] = self.partition_cols
            self.write_kwargs["existing_data_behavior"] = self.existing_data_behavior


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DistributedDatasetStorageConfig(StorageConfig):
    """
    Configuration class for distributed dataset storage, particularly for managing
    datasets that are partitioned and written using distributed storage frameworks
    like Apache Spark.

    Attributes:
        partitioned (bool): Indicates whether the dataset should be partitioned.
            Defaults to True.
        partition_cols (list): List of column names to use for partitioning the dataset.
            Defaults to an empty list, but is set to ["category"] if partitioned is True.
        mode (str): Defines the write mode for handling existing data. Common modes
            might include "overwrite", "append", "ignore", or "error".
            Defaults to "error".
        nlp (bool): Indicates whether the dataset is part of an NLP pipeline.

    Methods:
        __post_init__() -> None:
            Finalizes the storage configuration after object creation. Sets up the
            partitioning details and prepares keyword arguments for the write operation.
    """

    partitioned: bool = True
    partition_cols: list = field(default_factory=list)
    mode: str = "error"
    nlp: bool = False

    def __post_init__(self) -> None:
        """
        Initializes additional configuration settings after the object is created.
        Sets up partition columns and write options if the dataset is partitioned.

        The `write_kwargs` dictionary is configured to specify the write mode and
        the columns to partition by during write operations.

        Example:
            If `partitioned` is True, sets `partition_cols` to ["category"] and
            `write_kwargs` to:
                {"mode": self.mode, "partitionBy": ["category"]}

        Raises:
            ValueError: If an unsupported mode is provided (optional, based on validation needs).
        """
        self.write_kwargs = {"mode": self.mode}
        if self.partitioned:
            self.partition_cols = ["category"]
            self.write_kwargs["partitionBy"] = self.partition_cols
