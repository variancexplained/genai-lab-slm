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
# Modified   : Wednesday July 3rd 2024 06:56:23 am                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from __future__ import annotations

from appinsight.infrastructure.config.base import Config
from appinsight.infrastructure.config.env import EnvManager
from appinsight.infrastructure.persist.file.io import IOService


# ------------------------------------------------------------------------------------------------ #
#                                  DATASET CONFIG                                                  #
# ------------------------------------------------------------------------------------------------ #
class FileRecoveryConfig(Config):
    """Encapsulates read-only access to the Dataset configuration"""

    def __init__(
        self, env_mgr_cls: EnvManager = EnvManager, io_cls: IOService = IOService
    ) -> None:
        super().__init__(env_mgr_cls, io_cls)

    def get_file_location(self) -> str:
        """Returns the default file location for"""

    def get_db_recovery_folder(self) -> str:
        """Returns folder for database backup/recovery."""
        return self._config["recovery"]["db"]

    def get_file_recovery_folder(self) -> str:
        """Returns folder for file backup/recovery."""
        return self._config["recovery"]["file"]
