#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/dal/location.py                                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday September 24th 2024 01:04:56 pm                                             #
# Modified   : Saturday October 12th 2024 12:01:14 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""FilePath Service Module"""

import os


# ------------------------------------------------------------------------------------------------ #
class LocationService:
    """
    A service that manages and provides file locations for various resources such as datasets, files, models, and cache.
    Ensures that directories are created if they don't already exist.

    Attributes:
        workspace (str): The base workspace directory where all resource locations are stored.
        dataset_location (str): The relative path to the dataset storage location.
        file_location (str): The relative path to the general file storage location.
        model_location (str): The relative path to the model storage location.


    Methods:
        dataset_location: Returns the absolute path to the dataset storage location, creating the directory if necessary.
        file_location: Returns the absolute path to the general file storage location, creating the directory if necessary.
        model_location: Returns the absolute path to the model storage location, creating the directory if necessary.
    """

    def __init__(
        self,
        workspace: str,
        dataset_location: str,
        file_location: str,
        model_location: str,
    ) -> None:
        """
        Initializes the LocationService with base paths for datasets, files, models, and cache.

        Args:
            workspace (str): The base workspace directory.
            dataset_location (str): The relative path to the dataset directory.
            file_location (str): The relative path to the file directory.
            model_location (str): The relative path to the model directory.
        """
        self._workspace = workspace
        self._dataset_location = dataset_location
        self._file_location = file_location
        self._model_location = model_location

    @property
    def dataset_location(self) -> str:
        """
        Returns the absolute path to the dataset directory, ensuring the directory exists.

        Returns:
            str: The absolute path to the dataset directory.
        """
        location = os.path.join(self._workspace, self._dataset_location)
        os.makedirs(location, exist_ok=True)
        return location

    @property
    def file_location(self) -> str:
        """
        Returns the absolute path to the file directory, ensuring the directory exists.

        Returns:
            str: The absolute path to the file directory.
        """
        location = os.path.join(self._workspace, self._file_location)
        os.makedirs(location, exist_ok=True)
        return location

    @property
    def model_location(self) -> str:
        """
        Returns the absolute path to the model directory, ensuring the directory exists.

        Returns:
            str: The absolute path to the model directory.
        """
        location = os.path.join(self._workspace, self._model_location)
        os.makedirs(location, exist_ok=True)
        return location
