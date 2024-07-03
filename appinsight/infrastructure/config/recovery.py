#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/infrastructure/config/recovery.py                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday June 30th 2024 05:08:20 am                                                   #
# Modified   : Wednesday July 3rd 2024 02:52:02 pm                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from __future__ import annotations

from appinsight.infrastructure.config.base import Config
from appinsight.infrastructure.config.env import EnvManager
from appinsight.infrastructure.persist.file.io import IOService


# ------------------------------------------------------------------------------------------------ #
#                               FILE RECOVERY  CONFIG                                              #
# ------------------------------------------------------------------------------------------------ #
class FileRecoveryConfig(Config):
    """Encapsulates read-only access to the Dataset configuration"""

    def __init__(
        self, env_mgr_cls: EnvManager = EnvManager, io_cls: IOService = IOService
    ) -> None:
        super().__init__(env_mgr_cls, io_cls)

    def get_default_source(self) -> str:
        """Returns the default backup source"""
        return self._config["recovery"]["file"]["default_source"]

    def get_backup_directory(self) -> str:
        """Returns backup directory."""
        return self._config["recovery"]["file"]["backup_directory"]


# ------------------------------------------------------------------------------------------------ #
#                            DATABASE RECOVERY  CONFIG                                             #
# ------------------------------------------------------------------------------------------------ #
class DBRecoveryConfig(Config):
    """Encapsulates read-only access to the Dataset configuration"""

    def __init__(
        self, env_mgr_cls: EnvManager = EnvManager, io_cls: IOService = IOService
    ) -> None:
        super().__init__(env_mgr_cls, io_cls)

    def get_backup_source(self) -> str:
        """Returns the backup source"""
        return self._config["recovery"]["db"]["backup_source"]

    def get_backup_directory(self) -> str:
        """Returns backup directory."""
        return self._config["recovery"]["db"]["backup_directory"]
