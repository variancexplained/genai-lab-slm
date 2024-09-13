#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/storage/repo/base.py                                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday September 11th 2024 12:41:17 am                                           #
# Modified   : Friday September 13th 2024 12:01:34 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""McKinney Repo Module"""
import os
import shutil
from abc import abstractmethod
from enum import Enum
from typing import Any

import pandas as pd

from discover.domain.service.base.repo import Repo
from discover.domain.value_objects.lifecycle import Stage
from discover.infra.config.config import Config


# ------------------------------------------------------------------------------------------------ #
class FileFormat(Enum):
    CSV = ".csv"
    TSV = ".tsv"
    PICKLE = ".pickle"
    PARQUET = ".parquet"
    PARQUET_PARTITIONED = ""


# ------------------------------------------------------------------------------------------------ #
class ReviewRepo(Repo):
    """
    A base class for managing review data in various file formats.

    This class provides a generic interface for adding, retrieving,
    and removing data from the file system, while delegating the specific
    file reading and writing operations to subclasses.

    Attributes:
    -----------
    _file_format : FileFormat
        The format in which the data will be saved (CSV, TSV, Pickle, Parquet, etc.).
    _config : Config
        Configuration object for retrieving settings like base directory and environment.
    _basedir : str
        The base directory where the data files are stored.

    Methods:
    --------
    add(data: Any, stage: Stage, name: str) -> None
        Adds data to the repository by writing it to a file.

    get(stage: Stage, name: str) -> pd.DataFrame
        Retrieves data from the repository by reading from a file.

    remove(stage: Stage, name: str) -> None
        Removes a file from the repository.

    exists(stage: Stage, name: str) -> bool
        Checks whether the file exists in the repository.

    _get_filepath(stage: Stage, name: str) -> str
        Constructs the file path based on the stage, name, and file format.

    _read(filepath: str) -> Any
        Abstract method for reading data from a file. To be implemented by subclasses.

    _write(data: Any, filepath: str) -> Any
        Abstract method for writing data to a file. To be implemented by subclasses.
    """

    def __init__(
        self, file_format: FileFormat, config_cls: type[Config] = Config
    ) -> None:
        """
        Initializes the ReviewRepo with the specified file format and configuration.

        Parameters:
        -----------
        file_format : FileFormat
            The format in which the data will be stored (CSV, TSV, Pickle, Parquet, etc.).
        config_cls : type[Config], optional
            The configuration class that provides environment-specific settings, by default Config.
        """
        super().__init__()
        self._file_format = file_format
        self._config = config_cls()
        self._basedir = self._config.get_config(section="data").basedir

    def add(self, data: Any, stage: Stage, name: str) -> None:
        """
        Adds data to the repository by writing it to a file.

        Parameters:
        -----------
        data : Any
            The data to be written to the file.
        stage : str
            The stage in the data pipeline (e.g., "raw", "processed").
        name : str
            The name of the file.

        Raises:
        -------
        FileExistsError:
            If the file already exists in the specified path.
        """
        filepath = self._get_filepath(stage=stage, name=name)
        if os.path.exists(filepath):
            msg = f"File {filepath} cannot be created. It already exists."
            self._logger.exception(msg)
            raise FileExistsError(msg)

        self._write(filepath=filepath, data=data)

    def get(self, stage: Stage, name: str) -> pd.DataFrame:
        """
        Retrieves data from the repository by reading it from a file.

        Parameters:
        -----------
        stage : str
            The stage in the data pipeline (e.g., "raw", "processed").
        name : str
            The name of the file to be read.

        Returns:
        --------
        pd.DataFrame:
            The retrieved data as a pandas DataFrame (or other type based on implementation).
        """
        filepath = self._get_filepath(stage=stage, name=name)
        return self._read(filepath=filepath)

    def remove(self, stage: Stage, name: str) -> None:
        """
        Removes a file from the repository.

        Parameters:
        -----------
        stage : str
            The stage in the data pipeline (e.g., "raw", "processed").
        name : str
            The name of the file to be removed.

        Raises:
        -------
        FileNotFoundError:
            If the file to be removed is not found.
        OSError:
            If there is an issue removing a directory or file.
        """
        confirm = input(
            f"Removing {name} from {stage} stage is permanent. Confirm [Y/N]."
        )
        if confirm.lower() == "y":
            filepath = self._get_filepath(stage=stage, name=name)

            try:
                os.remove(filepath)
            except FileNotFoundError as e:
                msg = f"File {filepath} was not found.\n{e}"
                self._logger.exception(msg)
                raise
            except OSError:
                shutil.rmtree(filepath)
        else:
            self._logger.info(f"Removal of {name} from {stage} stage aborted.")

    def exists(self, stage: Stage, name: str) -> bool:
        """
        Checks if a file exists in the repository.

        Parameters:
        -----------
        stage : str
            The stage in the data pipeline (e.g., "raw", "processed").
        name : str
            The name of the file.

        Returns:
        --------
        bool:
            True if the file exists, False otherwise.
        """
        filepath = self._get_filepath(stage=stage, name=name)
        return os.path.exists(filepath)

    def _get_filepath(self, stage: Stage, name: str) -> str:
        """
        Constructs the file path based on the stage, name, and file format.

        Parameters:
        -----------
        stage : str
            The stage in the data pipeline (e.g., "raw", "processed").
        name : str
            The name of the file.

        Returns:
        --------
        str:
            The constructed file path.
        """
        env = self._config.get_environment()
        return (
            os.path.join(self._basedir, env, stage.value, name)
            + self._file_format.value
        )

    @abstractmethod
    def _read(self, filepath: str) -> Any:
        """
        Abstract method for reading data from a file. To be implemented by subclasses.

        Parameters:
        -----------
        filepath : str
            The file path to read from.

        Returns:
        --------
        Any:
            The data read from the file.
        """
        pass

    @abstractmethod
    def _write(self, data: Any, filepath: str) -> None:
        """
        Abstract method for writing data to a file. To be implemented by subclasses.

        Parameters:
        -----------
        data : Any
            The data to be written to the file.
        filepath : str
            The file path to write to.
        """
        pass
