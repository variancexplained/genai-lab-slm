#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/application/config/setup/database.py                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday July 3rd 2024 09:30:00 am                                                 #
# Modified   : Wednesday July 3rd 2024 10:16:53 am                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Application Layer - Setup -  Database Configuration Module."""
import os
from collections import defaultdict
from dataclasses import dataclass, field

from dotenv import load_dotenv

from appinsight.application.config.base import AppConfig

# ------------------------------------------------------------------------------------------------ #
load_dotenv()


# ------------------------------------------------------------------------------------------------ #
@dataclass
class AppDatabaseSetupConfig(AppConfig):
    """Contains the database configuration

    The file containing the configuration is stored in the .env file keyed
    by this class name. The config is read into the config member as
    a dictionary containing the configurations for the backup, profile,
    and dataset tables.

    """

    config: defaultdict[dict] = field(default_factory=lambda: defaultdict(dict))

    def __post_init__(self) -> None:
        # Configuration file names are organized by the classes that use them.
        config_file_key = self.__class__.__name__.upper()
        # Obtain the config file
        filepath = os.getenv(config_file_key)
        # Set the query for the command.
        with open(file=filepath) as file:
            self.config = file.read()
