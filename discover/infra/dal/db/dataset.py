#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/dal/db/dataset.py                                                   #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 07:41:04 pm                                              #
# Modified   : Tuesday October 8th 2024 10:51:24 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset DAO Module"""
import copy
import logging
import os
import shelve
from typing import Any, Optional

import pandas as pd

from discover.core.flow import PhaseDef, StageDef
from discover.element.dataset.define import Dataset
from discover.infra.config.reader import ConfigReader
from discover.infra.dal.base import DAO


# ------------------------------------------------------------------------------------------------ #
class DatasetDAO(DAO):
    """
    Data Access Object for managing datasets in a persistent storage using shelve.

    This class provides methods to create, read, and delete datasets, as well as
    to retrieve datasets by their associated phase and stage.

    Attributes:
        _config_reader (ConfigReader): Instance of the configuration reader.
        _db_path (str): Path to the shelve database for storing datasets.
        _logger (Logger): Logger for tracking operations performed by the DAO.

    Args:
        config_reader_cls (type): Class type of the ConfigReader to be instantiated.
                                   Defaults to ConfigReader.
    """

    def __init__(self, config_reader_cls: type[ConfigReader] = ConfigReader):
        self._config_reader = config_reader_cls()
        self._db_path = self._config_reader.get_config(section="ops").dataset
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def create(self, dataset: Dataset) -> Dataset:
        """
        Adds a new dataset to the persistent storage.

        Note: Dataset contents should be stored separately. This method ensures that
        dataset content is none before persisting in this key value database.

        Args:
            dataset (Dataset): The dataset object sans content to be stored.

        Returns:
            Dataset: Updated dataset with persisted datetime and cost.
        """
        dataset.persist()
        serialized_dataset = copy.deepcopy(dataset)
        serialized_dataset.content = (
            None  # Ensure dataset content is not persisted here.
        )
        with shelve.open(self._db_path, writeback=True) as db:
            db[str(dataset.id)] = serialized_dataset
        return dataset

    def read(self, id: int) -> Optional[Dataset]:
        """
        Retrieves a dataset by its unique identifier.

        Args:
            id (int): The unique identifier of the dataset.

        Returns:
            Optional[Dataset]: The dataset if found, otherwise None.
        """
        with shelve.open(self._db_path) as db:
            dataset = db.get(str(id), None)
            return dataset

    def read_all(self) -> pd.DataFrame:
        """
        Retrieves all datasets.

        Returns:
            pd.DataFrame: DataFrame containing datasets.
        """
        datasets_list = []
        with shelve.open(self._db_path) as db:
            for key in db:
                dataset = db[key]
                datasets_list.append(dataset.as_dict())

        if len(datasets_list) == 0:
            self._logger.info("No datasets found.")

        return pd.DataFrame(datasets_list)

    def read_by_phase(self, phase: PhaseDef) -> pd.DataFrame:
        """
        Retrieves all datasets associated with a specific phase.

        Args:
            phase (PhaseDef): The phase definition to filter datasets.

        Returns:
            pd.DataFrame: DataFrame containing datasets associated with the given phase.
        """
        return self._read_by_attribute("phase", phase)

    def read_by_stage(self, stage: StageDef) -> pd.DataFrame:
        """
        Retrieves all datasets associated with a specific stage.

        Args:
            stage (StageDef): The stage definition to filter datasets.

        Returns:
            pd.DataFrame: DataFrame containing datasets associated with the given stage.
        """
        return self._read_by_attribute("stage", stage)

    def exists(self, id: int) -> bool:
        """
        Checks if a dataset with the given ID exists in the database.

        Parameters:
        -----------
        id : int
            The unique identifier of the dataset to check.

        Returns:
        --------
        bool
            True if the dataset exists, False otherwise.
        """
        with shelve.open(self._db_path) as db:
            return str(id) in db

    def delete(self, id: int) -> None:
        """
        Deletes a dataset from the persistent storage.

        Args:
            id (int): The unique identifier of the dataset to delete.

        Returns:
            None
        """
        with shelve.open(self._db_path, writeback=True) as db:
            if str(id) in db:
                del db[str(id)]
            else:
                self._logger.warning(f"Dataset {id} not found for deletion.")

    def reset(self, force: bool = False) -> None:
        """
        Resets the dataset DAO.
        """
        if (
            force
            or "y"
            in input("Resetting the DatasetDAO is permanent. Confirm. [Y/N] ").lower()
        ):
            self._reset()

    def _reset(self) -> None:
        """Resets the dataset dao"""
        for ext in (".db", ".bak", ".dat", ".dir"):
            try:
                os.remove(self._db_path + ext)
            except FileNotFoundError:
                pass

    def _read_by_attribute(self, attribute: str, value: Any) -> pd.DataFrame:
        """
        Helper method to retrieve datasets based on a specified attribute.

        Args:
            attribute (str): The attribute to filter datasets by (e.g., 'phase', 'stage').
            value (Any): The value of the attribute to match.

        Returns:
            pd.DataFrame: DataFrame containing datasets that match the specified attribute.
        """
        datasets_list = []
        with shelve.open(self._db_path) as db:
            for key in db:
                dataset = db[key]
                if getattr(dataset, attribute) == value:
                    datasets_list.append(dataset.as_dict())

        if len(datasets_list) == 0:
            self._logger.info(f"No datasets found for {attribute}={value}.")

        return pd.DataFrame(datasets_list)
