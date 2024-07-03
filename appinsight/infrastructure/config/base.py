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
# Modified   : Wednesday July 3rd 2024 12:44:32 pm                                                 #
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
        Initializes the IO class with an environment manager and an IOService class.

        Args:
            env_mgr_cls (type[EnvManager], optional): Class for managing environments. Defaults to EnvManager.
        """
        # Instantiate an IOService and ENV Manager instances.
        self._io = io_cls()
        self._env_mgr = env_mgr_cls()
        # Load the environment variables from the .env file
        load_dotenv()
        # Set the key with which the config file will be obtained from the environment
        self._config_key = self.__class__.__name__.upper()
        # Get the config filepath for the environment, read the config filepath
        # and set the config property
        self._config = self._read()

    @property
    def config(self) -> str:
        return self._config

    def _read(self) -> dict:
        """Reads the underlying configuration file."""
        # Obtain the current environment from the environment variable
        env = self._env_mgr.get_environment()
        # Grab the config filepath from env variables.
        config_base_filepath = os.getenv(key=self._config_key)
        # Construct the path to the config file for the environment.
        config_filepath = os.path.join(config_base_filepath, f"{env}.yml")
        # Return the configuation for the environment
        return self._io.read(filepath=config_filepath)
