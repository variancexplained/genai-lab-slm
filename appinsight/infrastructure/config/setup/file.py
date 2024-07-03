#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/infrastructure/config/setup/file.py                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday July 3rd 2024 03:59:33 pm                                                 #
# Modified   : Wednesday July 3rd 2024 04:22:57 pm                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Database Setup Config"""
from __future__ import annotations

from appinsight.infrastructure.config.base import Config
from appinsight.infrastructure.config.env import EnvManager
from appinsight.infrastructure.persist.file.io import IOService


# ------------------------------------------------------------------------------------------------ #
#                               DATASET DB CONFIG                                                  #
# ------------------------------------------------------------------------------------------------ #
class DatasetFileSetupConfig(Config):
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
