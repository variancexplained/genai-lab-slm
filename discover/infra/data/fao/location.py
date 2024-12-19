#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/data/fal/location.py                                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday September 24th 2024 01:04:56 pm                                             #
# Modified   : Thursday December 19th 2024 10:23:14 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Fileset Location Service Module"""
import os


# ------------------------------------------------------------------------------------------------ #
class WorkspaceService:
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
        filepath = os.path.join(self._workspace, "files", filename)
        return filepath
