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
# Modified   : Tuesday July 2nd 2024 02:30:28 am                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from __future__ import annotations

import os
from abc import ABC

from dotenv import load_dotenv

from appinsight.infrastructure.persist.file.io import IOService
from appinsight.infrastructure.utils.env import EnvManager


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
        """Returns format in which datasets will be saved."""
        return self._config["dataset"]["format"]

    def get_file_ext(self) -> str:
        """Returns the file extension for datasets."""
        return self._config["dataset"]["file_ext"]

    def get_save_kwargs(self) -> dict:
        """Returns the dictionary of keyword arguments for persisting the dataset."""
        return self._config["dataset"]["save_kwargs"]
