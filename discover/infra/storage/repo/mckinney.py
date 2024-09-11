#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/storage/repo/mckinney.py                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday September 9th 2024 02:58:50 pm                                               #
# Modified   : Wednesday September 11th 2024 10:15:53 am                                           #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""McKinney Repo Module"""
import logging

import pandas as pd

from discover.infra.access.file_io import IOService
from discover.infra.config.config import Config
from discover.infra.storage.repo.base import FileFormat, ReviewRepo


# ------------------------------------------------------------------------------------------------ #
class McKinneyRepo(ReviewRepo):
    """
    A repository for managing review data using pandas. This class supports reading
    and writing pandas DataFrames in various formats, such as CSV, TSV, Pickle, and
    Parquet. It extends the base `ReviewRepo` class and implements pandas-specific
    file operations using an IO service.

    Attributes:
    -----------
    _io : IOService
        The IO service responsible for reading and writing files to the file system.
    _logger : logging.Logger
        Logger for handling log messages during file operations.

    Methods:
    --------
    _read(filepath: str) -> pd.DataFrame
        Reads review data from the file system and returns it as a pandas DataFrame.

    _write(data: pd.DataFrame, filepath: str) -> None
        Writes review data to the file system as a pandas DataFrame.
    """

    def __init__(
        self,
        file_format: FileFormat,
        config_cls: type[Config] = Config,
        io: type[IOService] = IOService,
    ) -> None:
        """
        Initializes the McKinneyRepo with a file format, configuration, and IO service.

        Parameters:
        -----------
        file_format : FileFormat
            The format in which the review data will be stored (e.g., CSV, TSV, Pickle, Parquet).
        config_cls : type[Config], optional
            The configuration class that provides environment-specific settings, by default Config.
        io : type[IOService], optional
            The IO service class responsible for file operations, by default IOService.
        """
        super().__init__(file_format=file_format, config_cls=config_cls)
        self._io = io()  # Initialize the IO service
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def _read(self, filepath: str) -> pd.DataFrame:
        """
        Reads review data from the file system using the IO service and returns it as a pandas DataFrame.

        Parameters:
        -----------
        filepath : str
            The full file path to the file to be read.

        Returns:
        --------
        pd.DataFrame:
            The review data read from the file as a pandas DataFrame.
        """
        return self._io.read(filepath=filepath)

    def _write(self, data: pd.DataFrame, filepath: str) -> None:
        """
        Writes the given review data to the file system using the IO service.

        Parameters:
        -----------
        data : pd.DataFrame
            The review data to be written to the file.
        filepath : str
            The full file path where the file will be saved.
        """
        self._io.write(filepath=filepath, data=data)
