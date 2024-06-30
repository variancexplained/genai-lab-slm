#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/infrastructure/utils/config.py                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday June 30th 2024 05:08:20 am                                                   #
# Modified   : Sunday June 30th 2024 07:17:10 am                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from __future__ import annotations

import os
from abc import ABC, abstractmethod

from appinsight.infrastructure.file.io import IOService
from appinsight.infrastructure.utils.env import EnvManager


# ------------------------------------------------------------------------------------------------ #
#                             CONFIGURATION MANAGER                                                #
# ------------------------------------------------------------------------------------------------ #
class ConfigurationManager:
    """Manager for retrieving specific configuration objects."""

    # Class variable to hold the mapping of config type strings to config classes
    __config_classes = {}

    @classmethod
    def register_config_class(cls, config_type: str, config_class: type[Config]):
        """
        Registers a configuration class for a given config type.

        Args:
            config_type (str): The type of configuration.
            config_class (type[Config]): The configuration class.
        """
        cls.__config_classes[config_type] = config_class

    def get_config(self, config_type: str) -> Config:
        """
        Returns the specific configuration object based on config_type.

        Args:
            config_type (str): The type of configuration.

        Returns:
            Config: The specific configuration object.
        """
        try:
            return self.__config_classes[config_type]
        except KeyError:
            raise ValueError(f"Unknown config type: {config_type}")


# ------------------------------------------------------------------------------------------------ #
#                                       CONFIG                                                     #
# ------------------------------------------------------------------------------------------------ #
class Config(ABC):
    """Abstract base class for configurations."""

    __basedir = "config"

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
        self._io = io_cls()
        self._env_mgr = env_mgr_cls()
        self._config = self.read()

    def read(self) -> dict:
        """Reads the underlying configuration file."""
        env = self._env_mgr.get_environment()
        filepath = os.path.join(self.__basedir, f"{env}.yml")
        return self._io.read(filepath=filepath)


# ------------------------------------------------------------------------------------------------ #
#                                  DATASET CONFIG                                                  #
# ------------------------------------------------------------------------------------------------ #
class DatasetConfig(Config):
    """Encapsulates read-only access to the Dataset configuration"""

    def __init__(
        self, env_mgr_cls: EnvManager = EnvManager, io_cls: IOService = IOService
    ) -> None:
        super().__init__(env_mgr_cls, io_cls)

    def get_basedir(self) -> str:
        """Returns the base directory for datasets."""
        return self._config["dataset"]["basedir"]

    def get_frac(self) -> float:
        """Returns the fraction of the full dataset to sample for this environment."""
        return self._config["dataset"]["frac"]

    def get_format(self) -> str:
        """Returns the base directory for datasets."""
        return self._config["dataset"]["format"]

    def get_save_kwargs(self) -> dict:
        """Returns the dictionary of keyword arguments for persisting the dataset."""
        return self._config["dataset"]["save_kwargs"]
