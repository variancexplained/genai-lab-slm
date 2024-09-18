#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/storage/service/file.py                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 15th 2024 04:21:04 am                                              #
# Modified   : Tuesday September 17th 2024 03:09:23 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""FileService Module"""
import logging

import pandas as pd
from pyspark.sql import DataFrame, SparkSession

from discover.domain.service.core.data import DataService, Reader, Writer
from discover.domain.service.core.monitor.profiler import profiler
from discover.domain.value_objects.config import DataConfig
from discover.domain.value_objects.data_structure import DataStructure
from discover.infra.repo.mckinney import McKinneyRepo
from discover.infra.repo.zaharia import ZahariaRepo


# ------------------------------------------------------------------------------------------------ #
class FileService(DataService):
    """
    FileService is responsible for providing appropriate Reader and Writer instances
    based on the data structure (pandas or Spark). It manages the interaction with the
    repositories and Spark sessions, ensuring the correct repository and session are used
    based on the provided configuration.

    Attributes:
    -----------
    _mckinney_repo_cls : type[McKinneyRepo]
        The repository class for handling pandas DataFrames (default is McKinneyRepo).

    _zaharia_repo_cls : type[ZahariaRepo]
        The repository class for handling Spark DataFrames (default is ZahariaRepo).

    _spark : SparkSession, optional
        A cached instance of SparkSession for Spark-based operations, initialized lazily.

    _logger : logging.Logger
        A logger instance used for logging errors and debugging information.

    Methods:
    --------
    get_reader(config: DataConfig) -> Reader:
        Returns a Reader instance (PandasReader or SparkReader) based on the data structure in the config.

    get_writer(config: DataConfig) -> Writer:
        Returns a Writer instance (PandasWriter or SparkWriter) based on the data structure in the config.

    _get_spark() -> SparkSession:
        Lazily initializes and returns a SparkSession, caching it for future use.
    """

    def __init__(
        self,
        mckinney_repo_cls: type[McKinneyRepo] = McKinneyRepo,
        zaharia_repo_cls: type[ZahariaRepo] = ZahariaRepo,
    ) -> None:
        """
        Initializes the FileService with default repository classes for pandas and Spark DataFrames.

        Parameters:
        -----------
        mckinney_repo_cls : type[McKinneyRepo], optional
            The repository class used for pandas DataFrames (default is McKinneyRepo).

        zaharia_repo_cls : type[ZahariaRepo], optional
            The repository class used for Spark DataFrames (default is ZahariaRepo).
        """
        super().__init__()
        self._mckinney_repo_cls = mckinney_repo_cls
        self._zaharia_repo_cls = zaharia_repo_cls
        self._spark = None
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def get_reader(self, config: DataConfig) -> Reader:
        """
        Returns a Reader instance based on the data structure specified in the configuration.

        Parameters:
        -----------
        config : DataConfig
            A configuration object containing details about the data structure (pandas or Spark).

        Returns:
        --------
        Reader
            A Reader instance (PandasReader or SparkReader) that can read data based on the specified structure.

        Raises:
        -------
        ValueError
            If an invalid data structure is provided in the config.
        """
        if config.data_structure == DataStructure.PANDAS:
            repo = self._mckinney_repo_cls()
            self._logger.info(f"Creating PandasReader for {config.stage.description}")
            return PandasReader(repo=repo, config=config)

        elif config.data_structure == DataStructure.SPARK:
            repo = self._zaharia_repo_cls(spark=self._get_spark())
            self._logger.info(f"Creating SparkReader for {config.stage.description}")
            return SparkReader(repo=repo, config=config)

        else:
            msg = f"Invalid DataStructure value {config.data_structure}."
            self._logger.exception(msg)
            raise ValueError(msg)

    def get_writer(self, config: DataConfig) -> Writer:
        """
        Returns a Writer instance based on the data structure specified in the configuration.

        Parameters:
        -----------
        config : DataConfig
            A configuration object containing details about the data structure (pandas or Spark).

        Returns:
        --------
        Writer
            A Writer instance (PandasWriter or SparkWriter) that can write data based on the specified structure.

        Raises:
        -------
        ValueError
            If an invalid data structure is provided in the config.
        """
        if config.data_structure == DataStructure.PANDAS:
            repo = self._mckinney_repo_cls()
            self._logger.info(f"Creating PandasWriter for {config.stage.description}")
            return PandasWriter(repo=repo, config=config)

        elif config.data_structure == DataStructure.SPARK:
            repo = self._zaharia_repo_cls(spark=self._get_spark())
            self._logger.info(f"Creating SparkWriter for {config.stage.description}")
            return SparkWriter(repo=repo, config=config)

        else:
            msg = f"Invalid DataStructure value {config.data_structure}."
            self._logger.exception(msg)
            raise ValueError(msg)

    def _get_spark(self) -> SparkSession:
        """
        Lazily initializes and returns a SparkSession to be used for Spark-based operations.
        Caches the SparkSession for reuse in subsequent requests.

        Returns:
        --------
        SparkSession
            A SparkSession instance for interacting with Spark DataFrames.

        Raises:
        -------
        Exception
            If the Spark session initialization fails.
        """
        if not self._spark:
            try:
                self._spark = (
                    SparkSession.builder.appName("appvocai-discover-spark-session")
                    .master("local[*]")
                    .getOrCreate()
                )
                self._logger.info("Successfully initialized Spark session.")
            except Exception as e:
                self._logger.error("Failed to initialize Spark session.", exc_info=True)
                raise e
        return self._spark


# ------------------------------------------------------------------------------------------------ #
#                                       PANDAS                                                     #
# ------------------------------------------------------------------------------------------------ #
class PandasReader(Reader):
    """
    PandasReader is responsible for reading data from a source using the McKinneyRepo (pandas repository).
    It retrieves data based on the provided configuration (stage, name, format) and returns it as a pandas DataFrame.

    Attributes:
    -----------
    _repo : McKinneyRepo
        The repository instance used to interact with the data source for pandas DataFrames.

    _config : DataConfig
        A configuration object containing metadata (stage, name, format) for fetching the data.

    _logger : logging.Logger
        Logger instance used for tracking read operations and error handling.

    Methods:
    --------
    read() -> pd.DataFrame:
        Reads data from the repository based on the provided configuration and returns a pandas DataFrame.
    """

    def __init__(self, repo: McKinneyRepo, config: DataConfig) -> None:
        """
        Initializes the PandasReader with a repository and a configuration object.

        Parameters:
        -----------
        repo : McKinneyRepo
            The repository instance for pandas DataFrames.

        config : DataConfig
            The configuration containing details such as stage, name, and format.
        """
        super().__init__(config=config)
        self._repo = repo  # Store the repository instance to access data.
        self._config = config  # Store the configuration for reading data.
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @profiler
    def read(self) -> pd.DataFrame:
        """
        Reads data from the repository based on the stage, name, and format from the configuration.

        Returns:
        --------
        pd.DataFrame
            A pandas DataFrame containing the requested data.

        Raises:
        -------
        ValueError
            If the configuration is invalid or the read operation fails.
        """
        self._validate_config()

        try:
            self._logger.info(
                f"Reading data: stage={self._config.stage.description}, name={self._config.name}, format={self._config.format.value}"
            )
            data = self._repo.get(
                stage=self._config.io_stage,
                name=self._config.name,
                format=self._config.format,
            )
            self._logger.info(f"Successfully read data from {self._config.name}.")
            return data
        except Exception as e:
            self._logger.error(f"Failed to read data: {e}", exc_info=True)
            raise ValueError(f"Failed to read data from {self._config.name}.") from e

    def _validate_config(self) -> None:
        """
        Validates the configuration to ensure it contains valid stage, name, and format.

        Raises:
        -------
        ValueError
            If the configuration contains invalid or missing values.
        """
        if not self._config.stage or not self._config.name or not self._config.format:
            self._logger.error("Invalid configuration: Missing stage, name, or format.")
            raise ValueError(
                "Invalid configuration: stage, name, and format are required."
            )
        self._logger.debug("Configuration is valid.")


# ------------------------------------------------------------------------------------------------ #
class PandasWriter(Writer):
    """
    PandasWriter is responsible for writing data to a repository using the McKinneyRepo (pandas repository).
    It stores a pandas DataFrame in the repository based on the provided configuration (stage, name, format).

    Attributes:
    -----------
    _repo : McKinneyRepo
        The repository instance used to write data to a storage backend for pandas DataFrames.

    _config : DataConfig
        A configuration object containing metadata (stage, name, format) for storing the data.

    _logger : logging.Logger
        Logger instance used for tracking write operations and error handling.

    Methods:
    --------
    write(data: pd.DataFrame) -> None:
        Writes the pandas DataFrame to the repository based on the provided configuration.
    """

    def __init__(self, repo: McKinneyRepo, config: DataConfig) -> None:
        """
        Initializes the PandasWriter with a repository and a configuration object.

        Parameters:
        -----------
        repo : McKinneyRepo
            The repository instance for pandas DataFrames.

        config : DataConfig
            The configuration containing details such as stage, name, and format.
        """
        super().__init__(config=config)
        self._repo = repo  # Store the repository instance to handle data storage.
        self._config = config  # Store the configuration for writing data.
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @profiler
    def write(self, data: pd.DataFrame) -> None:
        """
        Writes the given pandas DataFrame to the repository based on the stage, name, and format from the configuration.

        Parameters:
        -----------
        data : pd.DataFrame
            The pandas DataFrame to be stored in the repository.

        Raises:
        -------
        ValueError
            If the configuration is invalid or the write operation fails.
        """
        self._validate_config()

        try:
            self._logger.info(
                f"Writing data: stage={self._config.stage.description}, name={self._config.name}, format={self._config.format.value}"
            )
            self._repo.add(
                data=data,
                stage=self._config.io_stage,
                name=self._config.name,
                format=self._config.format,
                partition_cols=self._config.partition_cols,
                existing_data_behavior=self._config.existing_data_behavior,
            )
            self._logger.info(f"Successfully wrote data to {self._config.name}.")
        except Exception as e:
            self._logger.error(f"Failed to write data: {e}", exc_info=True)
            raise ValueError(f"Failed to write data to {self._config.name}.") from e

    def _validate_config(self) -> None:
        """
        Validates the configuration to ensure it contains valid stage, name, and format.

        Raises:
        -------
        ValueError
            If the configuration contains invalid or missing values.
        """
        if not self._config.stage or not self._config.name or not self._config.format:
            self._logger.error("Invalid configuration: Missing stage, name, or format.")
            raise ValueError(
                "Invalid configuration: stage, name, and format are required."
            )
        self._logger.debug("Configuration is valid.")


# ------------------------------------------------------------------------------------------------ #
#                                        SPARK                                                     #
# ------------------------------------------------------------------------------------------------ #
class SparkReader(Reader):
    """
    SparkReader is responsible for reading data from a source using the ZahariaRepo (Spark repository).
    It retrieves Spark DataFrames based on the provided configuration (stage, name, format) and returns them.

    Attributes:
    -----------
    _repo : ZahariaRepo
        The repository instance used to interact with the data source for Spark DataFrames.

    _config : DataConfig
        A configuration object containing metadata (stage, name, format) for fetching the data.

    _logger : logging.Logger
        Logger instance used for tracking read operations and error handling.

    Methods:
    --------
    read() -> DataFrame:
        Reads data from the repository based on the provided configuration and returns a Spark DataFrame.
    """

    def __init__(self, repo: ZahariaRepo, config: DataConfig) -> None:
        """
        Initializes the SparkReader with a repository and a configuration object.

        Parameters:
        -----------
        repo : ZahariaRepo
            The repository instance for Spark DataFrames.

        config : DataConfig
            The configuration containing details such as stage, name, and format.
        """
        super().__init__(config=config)
        self._repo = repo  # Store the repository instance for reading Spark DataFrames.
        self._config = config  # Store the configuration for reading data.
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @profiler
    def read(self) -> DataFrame:
        """
        Reads data from the repository based on the stage, name, and format from the configuration.

        Returns:
        --------
        DataFrame
            A Spark DataFrame containing the requested data.

        Raises:
        -------
        ValueError
            If the configuration is invalid or the read operation fails.
        """
        self._validate_config()

        try:
            self._logger.info(
                f"Reading data: stage={self._config.stage.description}, name={self._config.name}, format={self._config.format.value}"
            )
            data = self._repo.get(
                stage=self._config.io_stage,
                name=self._config.name,
                format=self._config.format,
            )
            self._logger.info(f"Successfully read data from {self._config.name}.")
            return data
        except Exception as e:
            self._logger.error(f"Failed to read data: {e}", exc_info=True)
            raise ValueError(f"Failed to read data from {self._config.name}.") from e

    def _validate_config(self) -> None:
        """
        Validates the configuration to ensure it contains valid stage, name, and format.

        Raises:
        -------
        ValueError
            If the configuration contains invalid or missing values.
        """
        if (
            not self._config.stage
            or not self._config.io_stage
            or not self._config.name
            or not self._config.format
        ):
            self._logger.error("Invalid configuration: Missing stage, name, or format.")
            raise ValueError(
                "Invalid configuration: stage, name, and format are required."
            )
        self._logger.debug("Configuration is valid.")


# ------------------------------------------------------------------------------------------------ #
class SparkWriter(Writer):
    """
    SparkWriter is responsible for writing data to a repository using the ZahariaRepo (Spark repository).
    It stores a Spark DataFrame in the repository based on the provided configuration (stage, name, format).

    Attributes:
    -----------
    _repo : ZahariaRepo
        The repository instance used to write data to a storage backend for Spark DataFrames.

    _config : DataConfig
        A configuration object containing metadata (stage, name, format) for storing the data.

    _logger : logging.Logger
        Logger instance used for tracking write operations and error handling.

    Methods:
    --------
    write(data: DataFrame) -> None:
        Writes the Spark DataFrame to the repository based on the provided configuration.
    """

    def __init__(self, repo: ZahariaRepo, config: DataConfig) -> None:
        """
        Initializes the SparkWriter with a repository and a configuration object.

        Parameters:
        -----------
        repo : ZahariaRepo
            The repository instance for Spark DataFrames.

        config : DataConfig
            The configuration containing details such as stage, name, and format.
        """
        super().__init__(config=config)
        self._repo = repo  # Store the repository instance for writing Spark DataFrames.
        self._config = config  # Store the configuration for writing data.
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @profiler
    def write(self, data: DataFrame) -> None:
        """
        Writes the given Spark DataFrame to the repository based on the stage, name, and format from the configuration.

        Parameters:
        -----------
        data : DataFrame
            The Spark DataFrame to be stored in the repository.

        Raises:
        -------
        ValueError
            If the configuration is invalid or the write operation fails.
        """
        self._validate_config()

        try:
            self._logger.info(
                f"Writing data: stage={self._config.stage.description}, name={self._config.name}, format={self._config.format.value}"
            )
            self._repo.add(
                data=data,
                stage=self._config.io_stage,
                name=self._config.name,
                format=self._config.format,
            )
            self._logger.info(f"Successfully wrote data to {self._config.name}.")
        except Exception as e:
            self._logger.error(f"Failed to write data: {e}", exc_info=True)
            raise ValueError(f"Failed to write data to {self._config.name}.") from e

    def _validate_config(self) -> None:
        """
        Validates the configuration to ensure it contains valid stage, name, and format.

        Raises:
        -------
        ValueError
            If the configuration contains invalid or missing values.
        """
        if (
            not self._config.stage
            or not self._config.io_stage
            or not self._config.name
            or not self._config.format
        ):
            self._logger.error("Invalid configuration: Missing stage, name, or format.")
            raise ValueError(
                "Invalid configuration: stage, name, and format are required."
            )
        self._logger.debug("Configuration is valid.")
