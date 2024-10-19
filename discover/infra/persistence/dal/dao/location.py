#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/persistence/dal/dao/location.py                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday October 16th 2024 10:24:35 pm                                             #
# Modified   : Friday October 18th 2024 07:18:36 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import os

from discover.infra.persistence.dal.base.location import LocationService


# ------------------------------------------------------------------------------------------------ #
class DatasetLocationService(LocationService):
    """
    A concrete implementation of the LocationService for datasets. This class constructs file paths
    for datasets based on the specified workspace and location.

    Attributes:
        _workspace (str): The base directory where datasets are stored.
        _location (str): The specific location or subdirectory within the workspace where the dataset files
                         will be stored.
    """

    def __init__(self, workspace: str, location: str) -> None:
        """
        Initializes the DatasetLocationService with the specified workspace and location.

        Args:
            workspace (str): The base directory where dataset files will be stored.
            location (str): The subdirectory or specific location within the workspace for the dataset files.
        """
        super().__init__()
        self._workspace = workspace
        self._location = location

    def get_filepath(self, *args, **kwargs):
        """
        Constructs and returns the file path for a dataset based on the workspace and location.

        Args:
            *args: Variable length argument list (unused).
            **kwargs: Arbitrary keyword arguments (unused).

        Returns:
            str: The file path for the dataset as a string.
        """
        filepath = os.path.join(self._workspace, self._location)
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        return filepath
