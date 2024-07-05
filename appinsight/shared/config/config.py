#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/shared/config/config.py                                                 #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday June 30th 2024 05:08:20 am                                                   #
# Modified   : Thursday July 4th 2024 10:10:54 pm                                                  #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

from appinsight import DataClass
from appinsight.shared.config.env import EnvManager
from appinsight.shared.persist.file.io import IOService


# ------------------------------------------------------------------------------------------------ #
#                                       CONFIG                                                     #
# ------------------------------------------------------------------------------------------------ #
class Config:
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
        self._config = self._read_app_config()
        self._aws = self._read_aws_config()
        self._aws_file = self._read_aws_file_config()

    @property
    def logging(self) -> dict:
        return self._config["logging"]

    @property
    def database(self) -> dict:
        return self._config["database"]

    @property
    def dataset(self) -> dict:
        return self._config["dataset"]

    @property
    def kws(self) -> dict:
        return self._config["kws"]

    @property
    def recovery(self) -> dict:
        return self._config["recovery"]

    @property
    def aws(self) -> DataClass:
        return self._aws

    @property
    def aws_file(self) -> DataClass:
        return self._aws_file

    def _read_app_config(self) -> dict:
        """Reads the underlying configuration file."""
        # Obtain the current environment from the environment variable
        env = self._env_mgr.get_environment()
        # Grab the config filepath from env variables.
        config_base_filepath = os.getenv(key=self._config_key)
        # Construct the path to the config file for the environment.
        config_filepath = os.path.join(config_base_filepath, f"{env}.yml")
        # Return the configuation for the environment
        return self._io.read(filepath=config_filepath)

    def _read_aws_config(self) -> DataClass:
        @dataclass
        class AWSConfig(DataClass):
            access_key: str = None
            secret_access_key: str = None
            region: str = None

            def __post_init__(self):
                self.access_key = os.getenv("AWS_ACCESS_KEY")
                self.secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
                self.region_name = os.getenv("AWS_REGION")

        return AWSConfig()

    def _read_aws_file_config(self) -> DataClass:
        @dataclass
        class AWSFileConfig(DataClass):
            bucket_name: str = None
            folder: str = None
            s3_key: str = None

            def __post_init__(self):
                self.bucket_name = os.getenv("AWS_BUCKET_NAME")
                self.folder = os.getenv("AWS_FOLDER_REVIEWS")
                self.s3_key = os.getenv("AWS_S3_KEY")

        return AWSFileConfig()
