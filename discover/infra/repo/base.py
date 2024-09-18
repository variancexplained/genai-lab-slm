#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/repo/base.py                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday September 11th 2024 12:41:17 am                                           #
# Modified   : Wednesday September 18th 2024 04:21:29 pm                                           #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""McKinney Repo Module"""
import os
import shutil
from abc import abstractmethod
from typing import Any, Optional

import pandas as pd

from discover.domain.base.repo import Repo
from discover.domain.value_objects.file import FileFormat
from discover.domain.value_objects.lifecycle import Phase, Stage
from discover.infra.config.reader import ConfigReader


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
    _config : ConfigReader
        ConfigReaderuration object for retrieving settings like base directory and environment.
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

    __phase: Phase = Phase.DATAPREP

    def __init__(self, config_reader_cls: type[ConfigReader] = ConfigReader) -> None:
        """
        Initializes the ReviewRepo with the specified file format and configuration.

        Parameters:
        -----------
        file_format : FileFormat
            The format in which the data will be stored (CSV, TSV, Pickle, Parquet, etc.).
        config_reader_cls : type[ConfigReader], optional
            The configuration class that provides environment-specific settings, by default ConfigReader.
        """
        super().__init__()
        self._config_reader = config_reader_cls()
        self._basedir = self._config_reader.get_config(
            section="workspace", namespace=False
        )
        self._file_ext = self._config_reader.get_config(section="dataset").file_ext

    def add(
        self,
        data: Any,
        stage: Stage,
        name: str,
        format: Optional[FileFormat] = None,
        **kwargs,
    ) -> None:
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
        filepath = self._get_filepath(stage=stage, name=name, format=format)
        if os.path.exists(filepath):
            msg = f"File {filepath} cannot be created. It already exists."
            self._logger.exception(msg)
            raise FileExistsError(msg)

        self._write(filepath=filepath, data=data, **kwargs)

    def get(
        self,
        stage: Stage,
        name: str,
        format: Optional[FileFormat] = None,
    ) -> pd.DataFrame:
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
        filepath = self._get_filepath(stage=stage, name=name, format=format)
        return self._read(filepath=filepath)

    def remove(
        self,
        stage: Stage,
        name: str,
        format: Optional[FileFormat] = None,
    ) -> None:
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
            filepath = self._get_filepath(stage=stage, name=name, format=format)

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

    def exists(
        self,
        stage: Stage,
        name: str,
        format: Optional[FileFormat] = None,
    ) -> bool:
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
        filepath = self._get_filepath(stage=stage, name=name, format=format)
        return os.path.exists(filepath)

    def _get_filepath(
        self, stage: Stage, name: str, format: Optional[FileFormat] = None
    ) -> str:
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
        return (
            os.path.join(self._basedir, self.__phase.value, stage.value, name)
            + self._file_ext
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
