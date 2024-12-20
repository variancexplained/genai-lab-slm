#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/container.py                                                              #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday September 9th 2024 04:54:25 pm                                               #
# Modified   : Friday December 20th 2024 01:52:29 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""AppVoCAI-Discover Dependency Container"""
# %%
from __future__ import annotations

import logging
import logging.config

from dependency_injector import containers, providers

from discover.infra.config.app import AppConfigReader
from discover.infra.data.dal import DatasetDAO
from discover.infra.data.fao.pandas import PandasParquetFAO
from discover.infra.data.fao.spark import SparkParquetFAO
from discover.infra.persistence.repo.dataset import DatasetRepo
from discover.infra.service.spark.session import SparkSessionPool
from discover.infra.workspace.base import Workspace


# ------------------------------------------------------------------------------------------------ #
#                                     WORKSPACE CONTAINER                                          #
# ------------------------------------------------------------------------------------------------ #
class WorkspaceContainer(containers.DeclarativeContainer):

    config = providers.Configuration()

    workspace = providers.Singletonw(Workspace, config=config)


# ------------------------------------------------------------------------------------------------ #
#                                      LOGGING CONTAINER                                           #
# ------------------------------------------------------------------------------------------------ #
class LoggingContainer(containers.DeclarativeContainer):

    config = providers.Configuration()

    main = providers.Resource(
        logging.config.dictConfig,
        config=config.logging,
    )


# ------------------------------------------------------------------------------------------------ #
#                                   SPARK CONTAINER                                                #
# ------------------------------------------------------------------------------------------------ #
class SparkContainer(containers.DeclarativeContainer):

    config = providers.Configuration()

    session_pool = providers.Singleton(SparkSessionPool, spark_config=config.spark)


# ------------------------------------------------------------------------------------------------ #
#                             DATASET REPOSITORY CONTAINER                                         #
# ------------------------------------------------------------------------------------------------ #
class DatasetRepositoryContainer(containers.DeclarativeContainer):

    config = providers.Configuration()

    # -------------------------------------------------------------------------------------------- #
    # Spark Session Pool
    spark_session_pool = providers.DependenciesContainer()
    # -------------------------------------------------------------------------------------------- #
    # Datasets Data Access Object
    ds_dao = providers.Singleton(
        DatasetDAO, workspace=config.workspace, dp_path=config.data.db.datasets
    )
    # -------------------------------------------------------------------------------------------- #
    # File Access Objects
    # -------------------------------------------------------------------------------------------- #
    # Pandas Parquet File Access Object
    pandas_parquet = providers.Singleton(
        PandasParquetFAO, config=config.data.fao.pandas.parquet
    )
    # -------------------------------------------------------------------------------------------- #
    # Spark Parquet File Access Object
    spark_parquet = providers.Singleton(
        SparkParquetFAO, config=config.data.fao.spark.parquet
    )
    # -------------------------------------------------------------------------------------------- #
    # Dataset Repo
    dataset_repo = providers.Singleton(
        DatasetRepo,
        dataset_dao=ds_dao,
        pandas_parquet=pandas_parquet,
        spark_parquet=spark_parquet,
        spark_session_pool=spark_session_pool,
    )


# ------------------------------------------------------------------------------------------------ #
#                                    APPLICATION CONTAINER                                         #
# ------------------------------------------------------------------------------------------------ #
class DiscoverContainer(containers.DeclarativeContainer):

    # Provide the Config class instance dynamically
    config_reader = providers.Singleton(AppConfigReader)

    # Provide the actual config dictionary by calling get_config()
    config = providers.Factory(
        lambda: DiscoverContainer.config_reader().get_config(namespace=False),
    )

    # Configure the logs by injecting the config data
    logs = providers.Container(LoggingContainer, config=config)

    # Configure spark session pool
    spark_session_pool = providers.Container(SparkContainer, config=config)

    # Dataset Repo
    repo = providers.Container(
        DatasetRepositoryContainer, spark_session_pool=spark_session_pool, config=config
    )
