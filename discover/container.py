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
# Modified   : Wednesday September 11th 2024 10:18:00 am                                           #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""AppVoCAI-Discover Dependency Container"""
from __future__ import annotations

import logging
import logging.config  # pragma: no cover

from dependency_injector import containers, providers

from discover.infra.config.config import Config
from discover.infra.storage.database.sqlite import SQLiteDB, SQLiteDBA
from discover.infra.storage.repo.profile import ProfileRepo

# ------------------------------------------------------------------------------------------------ #
# mypy: ignore-errors
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
#                                   REPO CONTAINER                                                 #
# ------------------------------------------------------------------------------------------------ #
class RepoContainer(containers.DeclarativeContainer):

    db = providers.DependenciesContainer()

    profile = providers.Singleton(ProfileRepo, database=db.sqlite)


# ------------------------------------------------------------------------------------------------ #
#                                 DATABASE CONTAINER                                               #
# ------------------------------------------------------------------------------------------------ #
class DatabaseContainer(containers.DeclarativeContainer):

    config = providers.Configuration()

    sqlite = providers.Singleton(
        SQLiteDB,
        connection_string=config.database.sqlite.url,
        location=config.database.sqlite.filepath,
    )

    admin = providers.Singleton(SQLiteDBA, database=sqlite)


# ------------------------------------------------------------------------------------------------ #
#                                    APPLICATION CONTAINER                                         #
# ------------------------------------------------------------------------------------------------ #
class DiscoverContainer(containers.DeclarativeContainer):

    # Provide the Config class instance dynamically
    config = providers.Singleton(Config)

    # Provide the actual config dictionary by calling get_config()
    config_data = providers.Factory(
        lambda: DiscoverContainer.config().get_config(namespace=False),
    )

    # Configure the logs by injecting the config data
    logs = providers.Container(LoggingContainer, config=config_data)

    # Configure the database by injecting the config data
    db = providers.Container(DatabaseContainer, config=config_data)

    # Configure the database by injecting the config data
    repo = providers.Container(RepoContainer, db=db)
