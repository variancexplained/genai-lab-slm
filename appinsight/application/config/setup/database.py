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
# Modified   : Wednesday July 3rd 2024 04:56:56 pm                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Application Layer - Setup -  Database Configuration Module."""
from dataclasses import dataclass

from appinsight.application.config.base import AppConfig
from appinsight.application.config.setup.backup import BackupDBSetupConfig
from appinsight.application.config.setup.dataset import DatasetDBSetupConfig
from appinsight.application.config.setup.profile import ProfileDBSetupConfig


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DBSetupPipelineConfig(AppConfig):
    """Contains the database configuration for the DBSetupPipeline"""

    profile: ProfileDBSetupConfig
    backup: BackupDBSetupConfig
    dataset: DatasetDBSetupConfig
