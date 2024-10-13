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
# Modified   : Sunday October 13th 2024 02:08:03 am                                                #
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

from discover.infra.config.reader import ConfigReader
from discover.infra.persistence.dao.dataset import DatasetDAO
from discover.infra.persistence.fao.centralized import CentralizedFileSystemFAO
from discover.infra.persistence.fao.distributed import DistributedFileSystemFAO
from discover.infra.persistence.fao.location import LocationService
from discover.infra.persistence.repo.dataset import DatasetRepo
from discover.infra.service.spark.session import SparkSessionPool

# ------------------------------------------------------------------------------------------------ #


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
#                                   REPO CONTAINER                                                 #
# ------------------------------------------------------------------------------------------------ #
class RepoContainer(containers.DeclarativeContainer):

    config = providers.Configuration()

    spark = providers.DependenciesContainer()

    location_service = providers.Singleton(
        LocationService,
        workspace=config.workspace,
        dataset_location=config.repository.dataset.objects.location,
        file_location=config.repository.dataset.files.location,
        model_location=config.repository.models.location,
    )

    # Data Access Object
    dao = providers.Singleton(DatasetDAO, location_service=location_service)

    # Centralized File System File Access Object
    fao_cfs = providers.Singleton(
        CentralizedFileSystemFAO,
        storage_config=config.repository.dataset.files.centralized,
    )

    # Distributed File System File Access Object
    fao_dfs = providers.Singleton(
        DistributedFileSystemFAO,
        storage_config=config.repository.dataset.files.distributed,
        session_pool=spark.session_pool,
    )

    # Dataset Repository
    dataset_repo = providers.Singleton(
        DatasetRepo,
        dataset_dao=dao,
        fao_cfs=fao_cfs,
        fao_dfs=fao_dfs,
        location_service=location_service,
        partitioned=config.repository.dataset.files.partitioned,
    )


# ------------------------------------------------------------------------------------------------ #
#                                    APPLICATION CONTAINER                                         #
# ------------------------------------------------------------------------------------------------ #
class DiscoverContainer(containers.DeclarativeContainer):

    # Provide the Config class instance dynamically
    config_reader = providers.Singleton(ConfigReader)

    # Provide the actual config dictionary by calling get_config()
    config = providers.Factory(
        lambda: DiscoverContainer.config_reader().get_config(namespace=False),
    )

    # Configure the logs by injecting the config data
    logs = providers.Container(LoggingContainer, config=config)

    # Configure spark session pool
    spark = providers.Container(SparkContainer, config=config)

    # Configure the repository by injecting the database.
    repo = providers.Container(RepoContainer, spark=spark, config=config)


# ------------------------------------------------------------------------------------------------ #
# if __name__ == "__main__":
#     container = DiscoverContainer()
#     container.init_resources()

#     assert container.config()["workspace"] == "workspace/test"
#     logging.debug("Test Log message")
