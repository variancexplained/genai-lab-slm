#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/persistence/dal/file/location.py                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday September 24th 2024 01:04:56 pm                                             #
# Modified   : Wednesday December 18th 2024 12:13:03 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Fileset Location Service Module"""

import os

from discover.infra.persistence.dal.base import LocationService


# ------------------------------------------------------------------------------------------------ #
class FilesetLocationService(LocationService):
    """Service to manage file locations for a workspace.

    This class provides methods for constructing file paths within a predefined
    workspace directory.

    Args:
        workspace (str): The root directory of the workspace.
    """

    def __init__(self, workspace: str) -> None:
        self._workspace = workspace

    def get_filepath(self, filename: str) -> str:
        """Get the full file path for a given filename within the workspace.

        Constructs the file path by appending the "fileset" directory and the
        provided filename to the workspace path. Ensures the directory exists.

        Args:
            filename (str): The name of the file.

        Returns:
            str: The constructed file path.

        Raises:
            OSError: If the directory creation fails.
        """
        filepath = os.path.join(self._workspace, "fileset", filename)
        os.makedirs(filepath, exist_ok=True)
        return filepath
