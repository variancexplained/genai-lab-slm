#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/dal/file/filepath.py                                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday September 24th 2024 01:04:56 pm                                             #
# Modified   : Tuesday September 24th 2024 02:12:56 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""FilePath Service Module"""

import os

from discover.infra.config.reader import ConfigReader


# ------------------------------------------------------------------------------------------------ #
class FilePathService:
    """
    A service class to manage and generate file paths by prepending a base directory from configuration.

    This service reads a configuration (e.g., workspace or environment) and uses it as the base directory for file paths.
    It provides a method to retrieve a complete file path by combining the base directory with a provided relative file path.

    Attributes:
    -----------
    _config_reader : ConfigReader
        An instance of the ConfigReader class, used to read configurations.
    _basedir : str
        The base directory obtained from the configuration (e.g., workspace path).

    Methods:
    --------
    get_filepath(filepath: str) -> str:
        Returns the full file path by combining the base directory with the provided relative path.
    """

    def __init__(self, config_reader_cls: type[ConfigReader] = ConfigReader) -> None:
        """
        Initializes FilePathService with a configuration reader and sets the base directory.

        Parameters:
        -----------
        config_reader_cls : type[ConfigReader], optional
            A class used for reading configurations (default is ConfigReader). This allows for
            custom configuration readers if needed.
        """
        self._config_reader = config_reader_cls()
        self._basedir = self._config_reader.get_config(
            section="workspace", namespace=False
        )

    def get_filepath(self, filepath: str) -> str:
        """
        Generates a complete file path by combining the base directory with the provided file path.

        Parameters:
        -----------
        filepath : str
            The relative file path to be combined with the base directory.

        Returns:
        --------
        str
            The full file path created by joining the base directory and the given file path.
        """
        fullpath = os.path.join(self._basedir, filepath)
        # Ensure the parent directory exists
        os.makedirs(os.path.dirname(fullpath), exist_ok=True)
        return fullpath
