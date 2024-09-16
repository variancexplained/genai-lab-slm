#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/application/service/data/dqa.py                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday September 14th 2024 07:54:12 pm                                            #
# Modified   : Monday September 16th 2024 02:24:02 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""DQA Service Module"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, List

from discover.application.base.service import ApplicationService
from discover.domain.service.core.cache import Cache
from discover.domain.service.core.data import Reader, Writer
from discover.domain.service.data.dqa.service import DQADomainService
from discover.domain.value_objects.config import DataConfig, ServiceConfig
from discover.domain.value_objects.file_format import FileFormat
from discover.domain.value_objects.lifecycle import Stage
from discover.infra.storage.service.file import FileService


# ------------------------------------------------------------------------------------------------ #
#                                 DATA QUALITY CONFIG                                              #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DQASourceDataConfig(DataConfig):
    """
    DQASourceDataConfig represents the configuration for the source data in the Data Quality Assessment (DQA) process.
    It defines the stage, file name, and format for the source data, which is typically in the INGEST stage.

    Attributes:
    -----------
    stage : Stage
        The stage of the data pipeline where the source data is located (default is Stage.INGEST).

    name : str
        The name of the dataset (default is "reviews").

    format : FileFormat
        The format of the source data file (default is FileFormat.PARQUET_PARTITIONED).

    Example:
    --------
    source_config = DQASourceDataConfig(stage=Stage.INGEST, name="reviews", format=FileFormat.PARQUET_PARTITIONED)
    """

    stage: Stage = Stage.INGEST
    name: str = "reviews"
    format: FileFormat = FileFormat.PARQUET_PARTITIONED


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DQATargetDataConfig(DataConfig):
    """
    DQATargetDataConfig represents the configuration for the target data in the Data Quality Assessment (DQA) process.
    It extends the base configuration with additional fields, such as partition columns, which are required for
    partitioned Parquet files.

    Attributes:
    -----------
    stage : Stage
        The stage of the data pipeline where the target data is located (default is Stage.DQA).

    name : str
        The name of the dataset (default is "reviews").

    format : FileFormat
        The format of the target data file (default is FileFormat.PARQUET_PARTITIONED).

    partition_cols : List[str]
        A list of columns to be used for partitioning the target data. This is required when using partitioned Parquet files.

    Methods:
    --------
    __post_init__() -> None:
        Ensures that partition columns are provided when the format is partitioned Parquet. Raises a ValueError otherwise.

    Example:
    --------
    target_config = DQATargetDataConfig(stage=Stage.DQA, name="reviews", format=FileFormat.PARQUET_PARTITIONED, partition_cols=["category"])
    """

    stage: Stage = Stage.DQA
    name: str = "reviews"
    format: FileFormat = FileFormat.PARQUET_PARTITIONED
    partition_cols: List[str] = field(default_factory=lambda: ["category"])

    def __post_init__(self):
        """
        Post-initialization validation to ensure that partition columns are specified when the format is
        FileFormat.PARQUET_PARTITIONED. Raises a ValueError if partition columns are missing for partitioned Parquet files.

        Raises:
        -------
        ValueError
            If partition columns are not provided for partitioned Parquet files.
        """
        if self.format == FileFormat.PARQUET_PARTITIONED and not self.partition_cols:
            raise ValueError(
                "Partition columns are required for partitioned Parquet files."
            )


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DQAConfig(ServiceConfig):
    """
    DQAConfig encapsulates the configuration for the Data Quality Assessment (DQA) stage of the data pipeline.
    It manages the initialization of the Reader and Writer for handling data, cache settings, and multiprocessing
    options to optimize the DQA process.

    Attributes:
    -----------
    stage : Stage
        The current stage of the data pipeline, default is Stage.DQA.

    reader : Reader
        The Reader instance used to read source data during the DQA process.

    writer : Writer
        The Writer instance used to write processed data during the DQA process.

    init_sort_by : str, optional
        The column by which the data is initially sorted (default is "id").

    cache_name : str, optional
        The name of the cache used for caching intermediate results (default is "dqa").

    cache_cls : type[Cache], optional
        The class used for cache handling (default is Cache).

    n_jobs : int, optional
        The number of parallel jobs used for multiprocessing during the DQA process (default is 18).
        Must be a positive integer.

    force : bool, optional
        A flag indicating whether to force the DQA process, even when certain conditions are not met (default is False).

    Methods:
    --------
    __post_init__() -> None:
        Ensures that `n_jobs` is a positive integer, raising a ValueError if the check fails.

    create(cls, source_data_config: DQASourceDataConfig, target_data_config: DQATargetDataConfig) -> DQAConfig:
        A class method that creates and returns a DQAConfig instance. It initializes the FileService and assigns
        the reader and writer based on the provided source and target data configurations.

    Example:
    --------
    # Example instantiation of the DQAConfig:
    source_config = DQASourceDataConfig(stage=Stage.INGEST, name="reviews", format=FileFormat.PARQUET_PARTITIONED)
    target_config = DQATargetDataConfig(stage=Stage.DQA, name="reviews", format=FileFormat.PARQUET_PARTITIONED, partition_cols=["category"])

    dqa_config = DQAConfig.create(source_data_config=source_config, target_data_config=target_config)
    """

    stage: Stage = Stage.DQA
    reader: Reader
    writer: Writer
    init_sort_by: str = "id"
    cache_name: str = "dqa"
    cache_cls: type[Cache] = Cache
    n_jobs: int = 18
    force: bool = False

    def __post_init__(self):
        """
        Ensures that `n_jobs` is a valid positive integer.

        Raises:
        -------
        ValueError:
            If `n_jobs` is not a positive integer, an exception is raised.
        """
        if not isinstance(self.n_jobs, int) or self.n_jobs <= 0:
            raise ValueError("n_jobs must be a positive integer.")

    @classmethod
    def create(
        cls,
        source_data_config: DQASourceDataConfig,
        target_data_config: DQATargetDataConfig,
    ) -> DQAConfig:
        """
        Class method to create and return a DQAConfig instance. This method initializes the reader and writer using
        the provided source and target data configurations, and manages errors during the initialization process.

        Parameters:
        -----------
        source_data_config : DQASourceDataConfig
            The configuration for the source data in the DQA process (e.g., reviews in the INGEST stage).

        target_data_config : DQATargetDataConfig
            The configuration for the target data in the DQA process (e.g., reviews in the DQA stage with partitioning).

        Returns:
        --------
        DQAConfig
            An instance of the DQAConfig class initialized with the appropriate reader and writer.

        Raises:
        -------
        ValueError:
            If there is an error initializing the reader or writer, a ValueError is raised with a message.

        Example:
        --------
        dqa_config = DQAConfig.create(source_data_config=source_config, target_data_config=target_config)
        """
        try:
            fs = FileService()
            reader = fs.get_reader(config=source_data_config)
            writer = fs.get_writer(config=target_data_config)
        except Exception as e:
            logging.error(f"Failed to create reader/writer: {e}")
            raise ValueError("Error initializing DQAConfig.") from e

        return cls(reader=reader, writer=writer)


# ------------------------------------------------------------------------------------------------ #
class DQAApplicationService(ApplicationService):
    """
    Application service responsible for managing the Data Quality Assessment (DQA) workflow.

    This service creates the necessary configuration, initializes the DQA domain service,
    and orchestrates the running of the domain service. It also logs important details about
    the initialization and execution process.

    Parameters:
    ----------
    config_cls : type[DQAConfig], optional
        The configuration class used to create the DQA configuration. Defaults to DQAConfig.
    source_data_config : DQASourceDataConfig, optional
        Configuration object for the source data. Defaults to a new DQASourceDataConfig instance.
    target_data_config : DQATargetDataConfig, optional
        Configuration object for the target data. Defaults to a new DQATargetDataConfig instance.
    **kwargs : dict
        Additional keyword arguments that may be passed to the service.

    Attributes:
    ----------
    _logger : logging.Logger
        Logger instance for logging events related to the application service.
    _domain_service : DQADomainService
        Domain service responsible for executing the DQA pipeline.
    _config : DQAConfig
        The configuration used by the service, created using the provided or default configuration class.

    Methods:
    -------
    run() -> Any:
        Executes the DQA domain service and returns the result.
    """

    def __init__(
        self,
        config_cls: type[
            DQAConfig
        ] = DQAConfig,  # Class for creating the configuration object, default is DQAConfig.
        source_data_config: DQASourceDataConfig = DQASourceDataConfig(),  # Source data configuration object.
        target_data_config: DQATargetDataConfig = DQATargetDataConfig(),  # Target data configuration object.
        **kwargs,  # Additional keyword arguments for flexibility.
    ) -> None:
        # Create the configuration object using the config_cls factory method, passing the source and target data configurations.
        config = config_cls.create(
            source_data_config=source_data_config, target_data_config=target_data_config
        )
        # Call the parent class constructor (ApplicationService) with the generated config.
        super().__init__(config=config)

        # Initialize a logger specific to this class, using the module name and class name.
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        # Initialize the domain service (DQADomainService) with the current configuration.
        self._domain_service = DQADomainService(config=self._config)

        # Log a message indicating that the DQAApplicationService has been initialized.
        self._logger.info(
            f"DQAApplicationService initialized with config: {self._config}"
        )

    def run(self) -> Any:
        # Delegate the run operation to the domain service and return its result.
        return self._domain_service.run()
