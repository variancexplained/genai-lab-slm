#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/infrastructure/config/base.py                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday June 30th 2024 05:08:20 am                                                   #
# Modified   : Tuesday July 2nd 2024 10:25:39 pm                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from __future__ import annotations

import os
from abc import ABC

from dotenv import load_dotenv

from appinsight.infrastructure.config.env import EnvManager
from appinsight.infrastructure.persist.file.io import IOService


# ------------------------------------------------------------------------------------------------ #
#                                       CONFIG                                                     #
# ------------------------------------------------------------------------------------------------ #
class Config(ABC):
    """Abstract base class for configurations."""

    def __init__(
        self,
        env_mgr_cls: type[EnvManager] = EnvManager,
        io_cls: type[IOService] = IOService,
    ) -> None:
        """
        Initializes the IO class with an environment manager.

        Args:
            env_mgr_cls (type[EnvManager], optional): Class for managing environments. Defaults to EnvManager.
        """
        load_dotenv()
        self._io = io_cls()
        self._env_mgr = env_mgr_cls()
        self._config = self.read()

    def read(self) -> dict:
        """Reads the underlying configuration file."""
        env = self._env_mgr.get_environment()
        basedir = os.getenv(key="CONFIG_DIRECTORY")
        filepath = os.path.join(basedir, f"{env}.yml")
        return self._io.read(filepath=filepath)
