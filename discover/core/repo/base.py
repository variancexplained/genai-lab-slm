#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/core/repo/base.py                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday September 11th 2024 12:41:17 am                                           #
# Modified   : Friday September 20th 2024 08:07:34 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""McKinney Repo Module"""
import os
import shutil
from abc import abstractmethod
from typing import Any

import pandas as pd

from discover.core.config.reader import ConfigReader
from discover.substance.base.repo import Repo
from discover.substance.entity.config.dataset import DatasetConfig


# ------------------------------------------------------------------------------------------------ #
class ReviewRepo(Repo):
    """
    Repository class for managing review datasets in the Data Preparation Phase.

    This class provides an interface to add, retrieve, remove, and check the existence
    of review datasets. It operates on a file-based system, storing and retrieving
    datasets based on configurations.
    """

    __ephase: EPhase = EPhase.DATAPREP

    def __init__(self, config_reader_cls: type[ConfigReader] = ConfigReader) -> None:
        """
        Initializes the ReviewRepo with the necessary configuration reader.

        Parameters:
        -----------
        config_reader_cls : type[ConfigReader], optional
            The class used to read configuration settings. Defaults to `ConfigReader`.

        Initializes the repository's base directory and file extension using the provided configuration.
        """
        super().__init__()
        self._config_reader = config_reader_cls()
        self._basedir = self._config_reader.get_config(
            section="workspace", namespace=False
        )
        self._file_ext = self._config_reader.get_config(section="dataset").file_ext

    def add(self, config: DatasetConfig, data: Any, **kwargs) -> None:
        """
        Adds a dataset to the repository if it does not already exist.

        Parameters:
        -----------
        config : DatasetConfig
            The configuration object specifying the dataset's properties.
        data : Any
            The data to be written to the repository.
        overwrite : bool
            Whether to overwrite existing data. Default = False
        kwargs : dict
            Additional parameters for writing the data.

        Raises:
        -------
        FileExistsError:
            If the dataset file already exists in the repository.

        This method writes the dataset to the specified file path, ensuring that the file does not already exist.
        """
        filepath = self._get_filepath(config=config)
        if not config.force and os.path.exists(filepath):
            msg = f"File {filepath} cannot be created. It already exists."
            self._logger.exception(msg)
            raise FileExistsError(msg)

        self._write(filepath=filepath, data=data, **config.kwargs)

    def get(self, config: DatasetConfig) -> pd.DataFrame:
        """
        Retrieves a dataset from the repository.

        Parameters:
        -----------
        config : DatasetConfig
            The configuration object specifying the dataset's properties.

        Returns:
        --------
        pd.DataFrame:
            The dataset as a pandas DataFrame.

        This method reads the dataset from the specified file path and returns it.
        """
        filepath = self._get_filepath(config=config)
        return self._read(filepath=filepath)

    def remove(self, config: DatasetConfig, ignore_errors: bool = True) -> None:
        """
        Removes a dataset from the repository.

        Parameters:
        -----------
        config : DatasetConfig
            The configuration object specifying the dataset to be removed.

        Prompts the user for confirmation before permanently removing the dataset.
        Raises an exception if the file is not found or an error occurs during removal.
        """
        confirm = input(
            f"Removing {config.name} from {config.estage.description} is permanent. Confirm [Y/N]."
        )
        if confirm.lower() == "y":
            filepath = self._get_filepath(config=config)

            try:
                os.remove(filepath)
            except FileNotFoundError as e:
                msg = f"File {filepath} was not found.\n{e}"
                self._logger.exception(msg)
                if not ignore_errors:
                    raise
            except OSError:
                shutil.rmtree(filepath, ignore_errors=ignore_errors)
        else:
            self._logger.info(
                f"Removal of {config.name} from {config.estage.description} aborted."
            )

    def exists(self, config: DatasetConfig) -> bool:
        """
        Checks if a dataset exists in the repository.

        Parameters:
        -----------
        config : DatasetConfig
            The configuration object specifying the dataset.

        Returns:
        --------
        bool:
            True if the dataset exists, False otherwise.

        This method checks whether the dataset file exists in the specified location.
        """
        filepath = self._get_filepath(config=config)
        return os.path.exists(filepath)

    def _get_filepath(self, config: DatasetConfig) -> str:
        """"""
        return (
            os.path.join(
                self._basedir, config.ephase.value, config.estage.value, config.name
            )
            + config.format.value
        )

    @abstractmethod
    def _read(self, filepath: str) -> Any:
        """"""
        pass

    @abstractmethod
    def _write(self, data: Any, filepath: str) -> None:
        """"""
        pass
