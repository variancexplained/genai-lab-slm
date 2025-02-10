#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab                                                                           #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/container.py                                                              #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday September 9th 2024 04:54:25 pm                                               #
# Modified   : Saturday February 8th 2025 10:43:01 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""GenAI-Lab     Dependency Container"""
# %%
from __future__ import annotations

import logging
import logging.config

from dependency_injector import containers, providers

from genailab.infra.config.app import AppConfigReader
from genailab.infra.persist.repo.dataset import DatasetRepo
from genailab.infra.persist.repo.file.factory import DataFrameIOFactory
from genailab.infra.persist.repo.file.fao import FAO
from genailab.infra.persist.repo.object.dao import DAO
from genailab.infra.persist.repo.object.rao import RAO
from genailab.infra.service.spark.pool import SparkSessionPool


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
class SparkSessionContainer(containers.DeclarativeContainer):

    config = providers.Configuration()

    session_pool = providers.Singleton(SparkSessionPool, spark_config=config.spark)


# ------------------------------------------------------------------------------------------------ #
#                                    REPO CONTAINER                                                #
# ------------------------------------------------------------------------------------------------ #
class IOContainer(containers.DeclarativeContainer):

    config = providers.Configuration()

    # -------------------------------------------------------------------------------------------- #
    iofactory = providers.Singleton(DataFrameIOFactory, config=config.io)

    fao = providers.Singleton(
        FAO,
        iofactory=iofactory,
    )

    dao = providers.Singleton(DAO, db_path=config.repository.dataset.dal)

    rao = providers.Singleton(RAO, registry_path=config.repository.dataset.ral)

    repo = providers.Singleton(
        DatasetRepo,
        location=config.repository.dataset.fal,
        rao=rao,
        dao=dao,
        fao=fao,
    )


# ------------------------------------------------------------------------------------------------ #
#                                  APPLICATION CONTAINER                                           #
# ------------------------------------------------------------------------------------------------ #
class GenAILabContainer(containers.DeclarativeContainer):

    # Provide the Config class instance dynamically
    config_reader = providers.Singleton(AppConfigReader)

    # Provide the actual config dictionary by calling get_config()
    config = providers.Factory(
        lambda: GenAILabContainer.config_reader().get_config(namespace=False),
    )

    # Configure the logs by injecting the config data
    logs = providers.Container(LoggingContainer, config=config)

    # Configure spark session pool
    spark = providers.Container(SparkSessionContainer, config=config)

    # IO Container
    io = providers.Container(IOContainer, config=config)
