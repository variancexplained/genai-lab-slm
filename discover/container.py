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
# Modified   : Thursday December 19th 2024 03:15:53 pm                                             #
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
from discover.infra.data.fao.location import WorkspaceService
from discover.infra.data.fao.pandas import PandasParquetFAO
from discover.infra.persistence.dal.fao.centralized import CentralizedFilesetDAL
from discover.infra.persistence.dal.fao.distributed import DistributedFilesetDAL
from discover.infra.persistence.repo.dataset import DatasetRepo
from discover.infra.service.spark.session import SparkSessionPool


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
#                                 DATA ACCESS CONTAINER                                            #
# ------------------------------------------------------------------------------------------------ #
class DataAccessContainer(containers.DeclarativeContainer):

    config = providers.Configuration()

    # Spark Session Pool
    spark = providers.DependenciesContainer()

    # Data Access Object
    dao = providers.Singleton(DatasetDAO, dp_path=config.data.dal.db)

    # Pandas Parquet File Access Object
    pandas_parquet = providers.Singleton(
        PandasParquetFAO, config=config.data.fao.pandas.parquet
    )
    # Pandas csv File Access Object
    pandas_csv = providers.Singleton(PandasCSVFAO, config=config.data.fal.pandas.csv)

    # File Location Services
    file_location_service = providers.Singleton(
        WorkspaceService, workspace=config.workspace
    )

    # File Access Object Service

    # Centralized File System File Access Object
    fao_cfs = providers.Singleton(
        CentralizedFilesetDAL,
        storage_config=config.persistence.data.files.centralized,
        location_service=file_location_service,
    )

    # Distributed File System File Access Object
    fao_dfs = providers.Singleton(
        DistributedFilesetDAL,
        storage_config=config.persistence.data.files.distributed,
        location_service=file_location_service,
        session_pool=spark.session_pool,
    )


# ------------------------------------------------------------------------------------------------ #
#                          OBJECT PERSISTENCE CONTAINER                                            #
# ------------------------------------------------------------------------------------------------ #
class ObjectPersistenceContainer(containers.DeclarativeContainer):

    config = providers.Configuration()

    fileset_repo = providers.DependenciesContainer()

    dataset_location_service = providers.Singleton(
        DALLocationService,
        workspace=config.workspace,
        location=config.persistence.data.datasets.location,
    )

    # Dataset Repository
    dataset_repo = providers.Singleton(
        DatasetRepo,
        dataset_dal=dal,
        fileset_repo=fileset_repo,
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
    spark = providers.Container(SparkContainer, config=config)

    # File Persistence Container
    fileset_persistence = providers.Container(
        FilePersistenceContainer, spark=spark, config=config
    )

    # Object Persistence Container
    object_persistence = providers.Container(
        ObjectPersistenceContainer,
        config=config,
    )


# ------------------------------------------------------------------------------------------------ #
# if __name__ == "__main__":
#     container = DiscoverContainer()
#     container.init_resources()

#     assert container.config()["workspace"] == "workspace/test"
#     logging.debug("Test Log message")
