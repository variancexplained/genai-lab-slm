#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/repo/config.py                                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday October 10th 2024 04:58:59 pm                                              #
# Modified   : Friday October 11th 2024 07:08:15 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Repository Config Module"""
from dataclasses import dataclass, field
from typing import Optional

from discover.element.dataset import StorageConfig
from discover.infra.config.reader import ConfigReader

# ------------------------------------------------------------------------------------------------ #
config_reader = ConfigReader()
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
            Defaults to an empty list, but is set to ["content"] if partitioned is True.
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
        self._validate()

        self.read_kwargs = {"engine": self.engine}
        self.write_kwargs = {
            "engine": self.engine,
            "compression": self.compression,
            "index": self.index,
            "row_group_size": self.row_group_size,
        }
        if self.partitioned:
            self.partition_cols = ["content"]
            self.write_kwargs["partition_cols"] = self.partition_cols
            self.write_kwargs["existing_data_behavior"] = self.existing_data_behavior

    def _validate(self) -> None:
        errors = []
        if not isinstance(self.partitioned, bool):
            msg = "Invalid value for partitioned. Must be a boolean value."
            errors.append(msg)

        if self.engine:
            if self.engine not in ["pyarrow", "auto", "fastparquet"]:
                msg = (
                    "Invalid engine. Must be 'pyarrow', 'auto', 'fastparquet', or None."
                )
                errors.append(msg)
        if self.compression:
            if self.compression not in ["snappy", "gzip", "brotli", "lz4", "zstd"]:
                msg = "Invalid compression. Supported options: 'snappy' 'gzip', 'brotli', 'lz4', 'zstd'. Use None for no compression."
                errors.append(msg)
        if not isinstance(self.index, bool):
            msg = f"Expected boolean value for index. Encountered {type(self.index).__name__}."
            errors.append(msg)

        if self.existing_data_behavior:
            if self.existing_data_behavior not in [
                "overwrite_or_ignore",
                "error",
                "delete_matching",
            ]:
                msg = "Invalid value for existing data behavior. Must be 'overwrite_or_ignore', 'error', 'delete_matching', or None"
                errors.append(msg)
        if self.row_group_size:
            if not isinstance(self.row_group_size, int):
                msg = "Invalid row_group_size. Must be an integer or None."
                errors.append(msg)

        self._log_and_raise(errors=errors)


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
            Defaults to an empty list, but is set to ["content"] if partitioned is True.
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
            If `partitioned` is True, sets `partition_cols` to ["content"] and
            `write_kwargs` to:
                {"mode": self.mode, "partitionBy": ["content"]}

        Raises:
            ValueError: If an unsupported mode is provided (optional, based on validation needs).
        """
        self._validate()
        if self.partitioned:
            self.partition_cols = ["content"]
            self.write_kwargs = {"mode": self.mode, "partitionBy": self.partition_cols}

    def _validate(self) -> None:
        errors = []
        if not isinstance(self.partitioned, bool):
            msg = "Invalid value for partitioned. Must be a boolean value."
            errors.append(msg)

        if self.mode:
            if self.mode not in [
                "append",
                "overwrite",
                "ignore",
                "error",
                "errorifexists",
            ]:
                msg = "Invalid mode. Must be 'append', 'overwrite', 'ignore', 'error', 'errorifexists', or None"
                errors.append(msg)

        self._log_and_raise(errors=errors)
