#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/application/config/setup/dataset.py                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday July 3rd 2024 04:17:06 pm                                                 #
# Modified   : Wednesday July 3rd 2024 04:55:03 pm                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Backup Database Setup Config"""
import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv

from appinsight.application.config.base import AppConfig
from appinsight.infrastructure.persist.file.io import IOService

# ------------------------------------------------------------------------------------------------ #
load_dotenv()


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatasetDBSetupConfig(AppConfig):
    """
    Configuration class for creating database tables.

    Attributes:
        name (str): The name of the table to be created, the "dataset" table.
        exists_sql (Optional[str]): SQL query to check if the table exists.
        drop_sql (Optional[str]): SQL query to drop the table.
        create_sql (Optional[str]): SQL query to create the table.
        force (bool): Flag to force the table creation if the table already exists.
    """

    name: str = None
    exists_sql: Optional[str] = None
    drop_sql: Optional[str] = None
    create_sql: Optional[str] = None
    force: bool = False

    def __post_init__(self) -> None:
        """
        Post-initialization method to load SQL configurations and force flag from a YAML file.
        """
        self.name = "dataset"

        config_key = self.__class__.__name__.upper()
        config = self._get_config(config_key=config_key)

        self.force = config["force"]

        # Get exists SQL
        with open(config["exists"]) as file:
            self.exists_sql = file.read()

        # Get drop SQL
        with open(config["drop"]) as file:
            self.drop_sql = file.read()

        # Get create SQL
        with open(config["create"]) as file:
            self.create_sql = file.read()

    def _get_config(self, config_key: str) -> dict:
        """
        Obtain the configuration from the designated YAML file.

        Args:
            config_key (str): The key to retrieve the configuration from the environment variable.

        Returns:
            dict: The configuration dictionary from the YAML file.
        """
        config_filepath = os.getenv(config_key)
        return IOService.read(filepath=config_filepath)["setup"]["database"][self.name]
