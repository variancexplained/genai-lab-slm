#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/repo/factory.py                                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday September 18th 2024 04:40:56 pm                                           #
# Modified   : Wednesday September 18th 2024 05:14:21 pm                                           #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import logging

from pyspark.sql import SparkSession

from discover.application.service.base.config import DataConfig
from discover.domain.base.repo import RepoFactory
from discover.domain.value_objects.data_structure import DataStructure
from discover.infra.repo.base import ReviewRepo
from discover.infra.repo.mckinney import McKinneyRepo
from discover.infra.repo.zaharia import ZahariaRepo


# ------------------------------------------------------------------------------------------------ #
class ReviewRepoFactory(RepoFactory):
    """
    Factory class for creating review repository instances based on the specified data structure.

    This class is responsible for creating the appropriate review repository based on the
    data structure (e.g., Pandas or Spark) defined in the configuration. It supports two
    types of repositories: `McKinneyRepo` for Pandas-based operations and `ZahariaRepo`
    for Spark-based operations.

    Attributes:
    -----------
    _mckinney_repo_cls : type[McKinneyRepo]
        Class reference for the McKinneyRepo, which handles Pandas-based review operations.

    _zaharia_repo_cls : type[ZahariaRepo]
        Class reference for the ZahariaRepo, which handles Spark-based review operations.

    _spark : SparkSession or None
        A cached Spark session instance, lazily initialized when needed for Spark operations.

    _logger : logging.Logger
        Logger instance used for logging events and errors related to repository creation and Spark session initialization.

    Methods:
    --------
    get_repo(config: DataConfig) -> ReviewRepo:
        Creates and returns an instance of the appropriate review repository based on the
        data structure specified in the `config` argument. If the data structure is Pandas,
        a `McKinneyRepo` is returned. If the data structure is Spark, a `ZahariaRepo` is
        returned, with a Spark session initialized if necessary.

        Parameters:
        -----------
        config : DataConfig
            A configuration object that provides details about the data structure to be used
            and other relevant configurations for repository creation.

        Returns:
        --------
        ReviewRepo:
            An instance of either `McKinneyRepo` (for Pandas) or `ZahariaRepo` (for Spark),
            depending on the configuration provided.

        Raises:
        -------
        ValueError:
            If the `config.data_structure` is neither Pandas nor Spark, an error is raised
            and logged.

    _get_spark() -> SparkSession:
        Lazily initializes and returns a Spark session. If the session is already initialized,
        it returns the cached instance. The method handles Spark session creation and logs
        any errors that occur during initialization.

        Returns:
        --------
        SparkSession:
            A Spark session instance used for Spark-based operations.

        Raises:
        -------
        Exception:
            If the Spark session initialization fails, an exception is raised and logged.
    """

    def __init__(
        self,
        mckinney_repo_cls: type[McKinneyRepo] = McKinneyRepo,
        zaharia_repo_cls: type[ZahariaRepo] = ZahariaRepo,
    ) -> None:
        super().__init__()
        self._mckinney_repo_cls = mckinney_repo_cls
        self._zaharia_repo_cls = zaharia_repo_cls
        self._spark = None

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def get_repo(self, config: DataConfig) -> ReviewRepo:
        """
        Creates and returns a review repository based on the data structure specified in the config.

        This method checks the `data_structure` field in the provided `config` and creates
        either a Pandas-based repository (`McKinneyRepo`) or a Spark-based repository (`ZahariaRepo`).

        Parameters:
        -----------
        config : DataConfig
            The configuration object that contains information about the data structure
            (Pandas or Spark) and other parameters for repository creation.

        Returns:
        --------
        ReviewRepo:
            The repository instance for handling reviews, either a `McKinneyRepo` (for Pandas)
            or a `ZahariaRepo` (for Spark).

        Raises:
        -------
        ValueError:
            Raised if the data structure specified in the config is neither Pandas nor Spark.
        """
        if config.data_structure == DataStructure.PANDAS:
            repo = self._mckinney_repo_cls()

        elif config.data_structure == DataStructure.SPARK:
            repo = self._zaharia_repo_cls(spark=self._get_spark())
        else:
            msg = f"Invalid DataStructure value {config.data_structure}."
            self._logger.exception(msg)
            raise ValueError(msg)

        self._logger.info(
            f"Created {repo.__class__.__name__} for {config.stage.description}"
        )
        return repo

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
            If the Spark session initialization fails, an exception is raised and logged.
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
