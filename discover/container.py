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
# Modified   : Wednesday January 22nd 2025 02:47:56 am                                             #
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
from discover.infra.persist.repo.dataset import DatasetRepo
from discover.infra.persist.repo.file.factory import DataFrameIOFactory
from discover.infra.persist.repo.file.fao import FAO
from discover.infra.persist.repo.object.dao import DAO
from discover.infra.persist.repo.object.flowstate import FlowState
from discover.infra.persist.repo.object.rao import RAO
from discover.infra.service.spark.pool import SparkSessionPool
from discover.infra.workspace.passport.idgen import IDGen
from discover.infra.workspace.passport.location import LocationService
from discover.infra.workspace.passport.version import VersionManager
from discover.infra.workspace.service import Workspace


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
#                                    REPO CONTAINER                                                #
# ------------------------------------------------------------------------------------------------ #
class IOContainer(containers.DeclarativeContainer):

    config = providers.Configuration()

    # -------------------------------------------------------------------------------------------- #
    iofactory = providers.Singleton(DataFrameIOFactory, config=config.fao)

    fao = providers.Singleton(
        FAO,
        iofactory=iofactory,
    )

    dao = providers.Singleton(DAO, db_path=config.workspace.assets.datasets.metadata)

    rao = providers.Singleton(
        RAO, registry_path=config.workspace.assets.datasets.registry
    )

    repo = providers.Singleton(
        DatasetRepo,
        rao=rao,
        dao=dao,
        fao=fao,
    )

    # -------------------------------------------------------------------------------------------- #
    flowstate = providers.Singleton(FlowState, db_path=config.workspace.ops.flowstate)


# ------------------------------------------------------------------------------------------------ #
#                                   WORKSPACE CONTAINER                                            #
# ------------------------------------------------------------------------------------------------ #
class WorkspaceContainer(containers.DeclarativeContainer):

    config = providers.Configuration()

    io = providers.DependenciesContainer()

    version_manager = providers.Singleton(
        VersionManager, version_db_path=config.workspace.ops.version
    )

    location_service = providers.Singleton(
        LocationService, files_location=config.workspace.assets.datasets.files
    )

    idgen = providers.Singleton(IDGen, version_manager=version_manager)

    service = providers.Singleton(
        Workspace,
        files=config.workspace.files,
        dataset_repo=io.dataset_repo,
        model_repo=io.model_repo,
        experiment_repo=io.experiment_repo,
        version_manager=version_manager,
        location_service=location_service,
        idgen=idgen,
    )


# ------------------------------------------------------------------------------------------------ #
#                                  APPLICATION CONTAINER                                           #
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

    # IO Container
    io = providers.Container(IOContainer, config=config)

    # Workspace container
    workspace = providers.Container(
        WorkspaceContainer,
        config=config,
        io=io,
    )
