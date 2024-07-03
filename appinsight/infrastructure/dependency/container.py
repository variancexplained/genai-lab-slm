#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.11                                                                             #
# Filename   : /appinsight/infrastructure/dependency/container.py                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday March 27th 2023 07:02:56 pm                                                  #
# Modified   : Wednesday July 3rd 2024 07:23:08 am                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
"""AppInsight Dependency Container"""
from __future__ import annotations

import logging
import logging.config  # pragma: no cover
import os

from dependency_injector import containers, providers
from dotenv import load_dotenv

from appinsight.application.data_prep.setup import CreateDatasetTask
from appinsight.infrastructure.instrumentation.repo import ProfilingDAL
from appinsight.infrastructure.persist.database.db import SQLiteDB
from appinsight.infrastructure.persist.database.dba import SQLiteDBA

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
#                                     DATABASE CONTAINER                                           #
# ------------------------------------------------------------------------------------------------ #
class DatabaseContainer(containers.DeclarativeContainer):

    config = providers.Configuration()

    sqlite = providers.Singleton(
        SQLiteDB,
        connection_string=config.database.sqlite.url,
        location=config.database.sqlite.filepath,
    )

    admin = providers.Singleton(SQLiteDBA, database=sqlite)

    dal = providers.Singleton(ProfilingDAL, database=sqlite)


# ------------------------------------------------------------------------------------------------ #
#                                     DATASET CONTAINER                                            #
# ------------------------------------------------------------------------------------------------ #
class DatasetContainer(containers.DeclarativeContainer):

    config = providers.Configuration()

    setup = providers.Singleton(CreateDatasetTask, config=config)


# ------------------------------------------------------------------------------------------------ #
#                                    APPLICATION CONTAINER                                         #
# ------------------------------------------------------------------------------------------------ #
class AppInsightContainer(containers.DeclarativeContainer):

    CONFIG_FILEPATHS = {
        "dev": "./config/dev.yml",
        "prod": "./config/prod.yml",
        "test": "./config/test.yml",
    }

    # Get the env from the environment variable.
    load_dotenv()
    env = os.getenv("ENV")

    # Get the configuration file
    filepath = CONFIG_FILEPATHS.get(env)
    # Configure the ccontainer
    config = providers.Configuration(yaml_files=[filepath])
    # Configure the logs
    logs = providers.Container(LoggingContainer, config=config)
    # Configure the database
    db = providers.Container(DatabaseContainer, config=config)
    # Configure the datasets
    dataset = providers.Container(DatasetContainer, config=config)
