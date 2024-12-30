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
# Modified   : Monday December 30th 2024 03:43:27 pm                                               #
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

from discover.asset.base.atype import AssetType
from discover.infra.config.app import AppConfigReader
from discover.infra.persist.dataframe.factory import DataFrameIOFactory
from discover.infra.persist.file.fao import FAO
from discover.infra.persist.object.dao import ShelveDAO
from discover.infra.persist.repo.dataset import DatasetRepo
from discover.infra.persist.repo.experiment import ExperimentRepo
from discover.infra.persist.repo.model import ModelRepo
from discover.infra.service.spark.pool import SparkSessionPool
from discover.infra.workspace.idgen import IDGen
from discover.infra.workspace.location import LocationService
from discover.infra.workspace.service import Workspace
from discover.infra.workspace.version import VersionManager


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
class RepoContainer(containers.DeclarativeContainer):

    config = providers.Configuration()

    # -------------------------------------------------------------------------------------------- #
    dataset_dao = providers.Singleton(
        ShelveDAO,
        location=config.workspace.location,
        db_path=config.workspace.metadata.datasets,
        asset_type=AssetType.DATASET,
    )
    fao = providers.Singleton(
        FAO,
        fao_config=config.fao,
        io_factory=DataFrameIOFactory,
    )

    dataset_repo = providers.Singleton(
        DatasetRepo,
        dao=dataset_dao,
        fao=fao,
    )

    # -------------------------------------------------------------------------------------------- #
    model_dao = providers.Singleton(
        ShelveDAO,
        location=config.workspace.location,
        db_path=config.workspace.metadata.models,
        asset_type=AssetType.MODEL,
    )

    model_repo = providers.Singleton(ModelRepo, dao=model_dao)

    # -------------------------------------------------------------------------------------------- #
    experiment_dao = providers.Singleton(
        ShelveDAO,
        location=config.workspace.location,
        db_path=config.workspace.metadata.experiments,
        asset_type=AssetType.EXPERIMENT,
    )

    experiment_repo = providers.Singleton(ExperimentRepo, dao=experiment_dao)


# ------------------------------------------------------------------------------------------------ #
#                                   WORKSPACE CONTAINER                                            #
# ------------------------------------------------------------------------------------------------ #
class WorkspaceContainer(containers.DeclarativeContainer):

    config = providers.Configuration()

    repo = providers.DependenciesContainer()

    version_manager = providers.Singleton(
        VersionManager, version_db_path=config.workspace.ops.version
    )

    location_service = providers.Singleton(LocationService, config=config.workspace)

    idgen = providers.Singleton(IDGen, version_manager=version_manager)

    service = providers.Singleton(
        Workspace,
        config=config.workspace,
        dataset_repo=repo.dataset_repo,
        model_repo=repo.model_repo,
        experiment_repo=repo.experiment_repo,
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

    # Data Access Object Container
    repo = providers.Container(RepoContainer, config=config)

    # Workspace container
    workspace = providers.Container(
        WorkspaceContainer,
        config=config,
        repo=repo,
    )
