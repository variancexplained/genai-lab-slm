#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/infrastructure/persist/repo.py                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday July 1st 2024 05:08:52 am                                                    #
# Modified   : Tuesday July 2nd 2024 08:04:38 pm                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Repository Implementation Module"""
import logging

import pandas as pd
from dependency_injector.wiring import Provide, inject
from sqlalchemy import delete, insert

from appinsight.container import AppInsightContainer
from appinsight.domain.dataset import Dataset
from appinsight.domain.enums import Stage
from appinsight.domain.repo import Repo
from appinsight.infrastructure.persist.database.base import Database
from appinsight.infrastructure.persist.file.filesystem import FileSystem
from appinsight.infrastructure.utils.config import DatasetConfig


# ------------------------------------------------------------------------------------------------ #
class DatasetRepo(Repo):
    """
    Abstract base class defining the interface for repositories.

    Attributes:
        registry (pd.DataFrame): Returns the repository registry.

    Examples:
        >>> repo = DatasetRepo()
        >>> dataset = Dataset(name="example", content=pd.DataFrame())
        >>> added_dataset = repo.add(dataset)
        >>> retrieved_dataset = repo.get(added_dataset.oid)
        >>> repo.remove(added_dataset.oid)
    """

    @inject
    def __init__(
        self,
        fs_cls: type[FileSystem] = FileSystem,
        db: Database = Provide[AppInsightContainer.db.sqlite],
        config_cls: type[DatasetConfig] = DatasetConfig,
    ) -> None:
        """
        Initializes the DatasetRepo with file system, database, and configuration classes.

        Args:
            fs_cls (type[FileSystem]): The FileSystem class to use. Default is FileSystem.
            db_cls (type[SQLiteDB]): The SQLiteDB class to use. Default is SQLiteDB.
            config_cls (type[DatasetConfig]): The DatasetConfig class to use. Default is DatasetConfig.
        """
        super().__init__()
        self._fs = fs_cls()
        self._db = db
        self._config = config_cls()
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def __len__(self) -> int:
        return len(self.registry)

    @property
    def registry(self) -> pd.DataFrame:
        """Returns the repository registry."""
        return self._get_registry()

    def add(self, dataset: Dataset) -> Dataset:
        """
        Adds a dataset to the repository.

        Args:
            dataset (Dataset): The Dataset object to be added.

        Returns:
            Dataset: The dataset with an object id.

        Examples:
            >>> repo = DatasetRepo()
            >>> dataset = Dataset(name="example", content=pd.DataFrame())
            >>> added_dataset = repo.add(dataset)
            >>> print(added_dataset.oid)
        """
        dataset.validate()
        directory, filename = self._get_location(stage=dataset.stage, name=dataset.name)
        self._persist_content(
            directory=directory, filename=filename, content=dataset.content
        )
        dataset.oid = self._persist_metadata(
            dataset=dataset, directory=directory, filename=filename
        )
        return dataset

    def get(self, oid: int) -> Dataset:
        """
        Returns a dataset from the repository by object id.

        Args:
            oid (int): The object id of the dataset.

        Returns:
            Dataset: The dataset object.

        Examples:
            >>> repo = DatasetRepo()
            >>> dataset = repo.get(1)
            >>> print(dataset.name)
        """
        dataset = self._get_metadata(oid)
        directory, filename = self._get_location(
            stage=dataset["stage"], name=dataset["name"]
        )
        dataset["created"] = pd.to_datetime(dataset["created"])
        dataset["content"] = self._fs.read(directory=directory, filename=filename)
        return Dataset(**dataset)

    def remove(self, oid: int) -> None:
        """
        Removes a dataset from the repository by object id.

        Args:
            oid (int): The object id of the dataset.

        Examples:
            >>> repo = DatasetRepo()
            >>> repo.remove(1)
        """
        delete = input(
            "This will remove all metadata and content from the repository. Do you wish to proceed? [yes/no] "
        )
        if delete.lower() == "yes":
            dataset = self._get_metadata(oid)
            directory, filename = self._get_location(
                stage=dataset["stage"], name=dataset["name"]
            )
            self._fs.delete(directory=directory, filename=filename)
            query = "DELETE FROM dataset WHERE oid = ?;"
            params = (oid,)
            self._db.command(query=query, params=params)
            print(f"Dataset {oid} has been removed from the repository")
        else:
            print(f"Remove of dataset {oid} aborted.")

    def _get_metadata(self, oid: int) -> dict:
        """Returns metadata from the database as a dictionary."""
        query = """SELECT * FROM dataset WHERE oid = ?;"""
        params = (oid,)
        df = self._db.query(query=query, params=params)
        try:
            return df.iloc[0].to_dict()
        except IndexError as ie:
            msg = f"Metadata for oid: {oid} not found.\n{ie}"
            self._logger.exception(msg)
            raise ValueError(msg)

    def _get_registry(self) -> pd.DataFrame:
        """Returns the registry."""
        query = """SELECT * FROM dataset;"""
        return self._db.query(query=query)

    def _persist_content(
        self, directory: str, filename: str, content: pd.DataFrame
    ) -> None:
        save_kwargs = self._config.get_save_kwargs()
        self._fs.create(
            data=content, directory=directory, filename=filename, **save_kwargs
        )

    def _persist_metadata(self, dataset: Dataset, directory: str, filename: str) -> int:
        """Persists metadata to the database."""

        query = """INSERT INTO dataset (name, description, phase, stage, size, nrows, ncols, creator, created)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);"""
        data = dataset.as_dict()
        params = {
            "name": data["name"],
            "description": data["description"],
            "phase": data["phase"],
            "stage": data["stage"],
            "size": data["size"],
            "nrows": data["nrows"],
            "ncols": data["ncols"],
            "creator": data["creator"],
            "created": data["created"],
        }

        self._logger.debug(f"\nParams in persist_metadata: {params}")
        return self._db.create(query=query, params=params)

    def _get_location(self, stage: str, name: str) -> tuple[str, str]:
        """Returns the directory and filename for the dataset."""
        directory = Stage[stage].value
        file_ext = self._config.get_file_ext()
        filename = f"{name}{file_ext}"
        return directory, filename
