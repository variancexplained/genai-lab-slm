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
# Modified   : Tuesday December 24th 2024 02:23:40 am                                              #
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

from discover.core.asset import AssetType
from discover.infra.config.app import AppConfigReader
from discover.infra.persist.object.dao import ShelveDAO
from discover.infra.persist.repo.dataset import DatasetRepo
from discover.infra.persist.repo.experiment import ExperimentRepo
from discover.infra.persist.repo.inference import InferenceRepo
from discover.infra.persist.repo.model import ModelRepo
from discover.infra.service.spark.pool import SparkSessionPool
from discover.infra.workspace.service import WorkspaceService


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
        db_path=config.workspace.assets.datasets,
        asset_type=AssetType.DATASET,
    )
    dataset_repo = providers.Singleton(DatasetRepo, dao=dataset_dao)

    # -------------------------------------------------------------------------------------------- #
    model_dao = providers.Singleton(
        ShelveDAO,
        location=config.workspace.location,
        db_path=config.workspace.assets.models,
        asset_type=AssetType.MODEL,
    )

    model_repo = providers.Singleton(ModelRepo, dao=model_dao)

    # -------------------------------------------------------------------------------------------- #
    inference_dao = providers.Singleton(
        ShelveDAO,
        location=config.workspace.location,
        db_path=config.workspace.assets.inference,
        asset_type=AssetType.INFERENCE,
    )

    inference_repo = providers.Singleton(InferenceRepo, dao=inference_dao)

    # -------------------------------------------------------------------------------------------- #
    experiment_dao = providers.Singleton(
        ShelveDAO,
        location=config.workspace.location,
        db_path=config.workspace.assets.experiments,
        asset_type=AssetType.EXPERIMENT,
    )

    experiment_repo = providers.Singleton(ExperimentRepo, dao=experiment_dao)


# ------------------------------------------------------------------------------------------------ #
#                                   WORKSPACE CONTAINER                                            #
# ------------------------------------------------------------------------------------------------ #
class WorkspaceContainer(containers.DeclarativeContainer):

    config = providers.Configuration()

    repo = providers.DependenciesContainer()

    service = providers.Singleton(
        WorkspaceService,
        config=config.workspace,
        dataset_repo=repo.dataset_repo,
        model_repo=repo.model_repo,
        inference_repo=repo.inference_repo,
        experiment_repo=repo.experiment_repo,
    )


# # ------------------------------------------------------------------------------------------------ #
# #                                    FACTORY CONTAINER                                             #
# # ------------------------------------------------------------------------------------------------ #
# class FactoryContainer(containers.DeclarativeContainer):

#     config = providers.Configuration()

#     spark = providers.DependenciesContainer()
#     workspace = providers.DependenciesContainer()

#     dataset_factory = providers.Singleton(
#         DatasetFactory,
#         fal_config=config.fal,
#         workspace_service=workspace.service,
#         spark_session_pool=spark.session_pool,
#     )


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

    # # Factory Container
    # factory = providers.Container(
    #     FactoryContainer,
    #     config=config,
    #     spark=spark,
    #     workspace=workspace,
    # )
