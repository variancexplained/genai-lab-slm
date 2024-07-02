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
# Modified   : Tuesday July 2nd 2024 04:53:54 am                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Repository Implementation Module"""
import logging

import pandas as pd

from appinsight.domain.dataset import Dataset
from appinsight.domain.enums import Stage
from appinsight.domain.repo import Repo
from appinsight.infrastructure.persist.database.db import SQLiteDB
from appinsight.infrastructure.persist.file.filesystem import FileSystem
from appinsight.infrastructure.utils.config import DatasetConfig


# ------------------------------------------------------------------------------------------------ #
class DatasetRepo(Repo):
    """Abstract base class defining the interface for repositories."""

    def __init__(
        self,
        fs_cls: type[FileSystem] = FileSystem,
        db_cls: type[SQLiteDB] = SQLiteDB,
        config_cls: type[DatasetConfig] = DatasetConfig,
    ) -> None:
        super().__init__()
        self._fs = fs_cls()
        self._db = db_cls()
        self._config = config_cls()
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @property
    def registry(self) -> pd.DataFrame:
        """Returns the repository registry"""
        return self._get_registry()

    def add(self, dataset: Dataset) -> Dataset:
        """Adds an dataset to the repository.

        Args:
            dataset (Dataset): The Dataset object to be added.

        Returns the dataset with an object id.
        """
        dataset.validate()
        directory, filename = self._persist_content(dataset=dataset)
        dataset.oid = self._persist_metadata(
            dataset=dataset, directory=directory, filename=filename
        )
        return dataset

    def get(self, oid: int) -> Dataset:
        """Returns an item from the repository."""
        # Obtain metadata from the database.
        df = self._get_metadata(oid)
        # Obtain content
        content = self._fs.read(directory=df["directory"], filename=df["filename"])
        # Constitute the dataset object
        return Dataset.from_repo(df=df, content=content)

    def remove(self, oid: int) -> None:
        """Removes an entity from the repository.

        Args:
            oid (int): The object id.
        """
        delete = input(
            "This will remove all metadata and content from the repository. Do you wish to proceed? [yes/no]"
        )
        if delete.lower() == "yes":
            # Obtain the metadata containing directory and filename for the object
            df = self._get_metadata(oid)
            # Delete the content from the file.
            self._fs.delete(directory=df["directory"], filename=df["filename"])
            # Remove the item from the database.
            query = "DELETE FROM dataset WHERE oid = ?;"
            params = (oid,)
            self._db.command(query=query, params=params)
            print(f"Dataset {oid} has been removed from the repository")
        else:
            print(f"Remove of dataset {oid} aborted.")

    def _get_metadata(self, oid: int) -> pd.DataFrame:
        """Returns metadata from the database."""
        query = """SELECT * FROM dataset WHERE oid = ?;"""
        params = (oid,)
        df = self._db.query(query=query, params=params)
        self._validate_db_result(df)
        df = df.iloc[0]
        return df

    def _get_registry(self) -> pd.DataFrame:
        """Returns the registry"""
        query = """SELECT * FROM dataset;"""
        return self._db.query(query=query)

    def _persist_content(self, dataset: Dataset) -> None:
        """Persists the dataset content to file."""
        directory, filename = self._get_location(dataset=dataset)

        save_kwargs = self._config.get_save_kwargs()

        # Persist the content, returning the filepath
        self._fs.create(
            data=dataset.content,
            directory=directory,
            filename=filename,
            kwargs=save_kwargs,
        )
        return directory, filename

    def _persist_metadata(self, dataset: Dataset, directory: str, filename: str) -> int:
        """Persists metadata to the database."""

        query = """INSERT INTO dataset (name, description, phase, stage, size, nrows, ncols, directory, filename, creator, created)
        VALUES
        (?,?,?,?,?,?,?,?,?,?);"""
        data = dataset.to_dict()
        params = (
            data["name"],
            data["description"],
            data["phase"],
            data["stage"],
            data["size"],
            data["nrows"],
            data["ncols"],
            directory,
            filename,
            data["creator"],
            data["created"],
        )
        return self._db.create(query=query, params=params)

    def _validate_db_result(self, df: pd.DataFrame) -> None:
        """Ensures the database result is a valid, non-empty dataframe"""
        if not isinstance(df, pd.DataFrame):
            msg = "Error occurred while reading from the database."
            raise RuntimeError(msg)
        elif len(df) == 0:
            msg = "No records found for the query"
            raise RuntimeError(msg)

    def _get_location(self, dataset: Dataset) -> tuple[str, str]:
        """Returns the directory and filename for the dataset."""
        # Obtain the file extension from the dataset from config.
        file_ext = self._config.get_file_ext()
        # Obtain the directory for the dataset stage.
        directory = Stage[dataset.stage]
        # Filename is the dataset name with the file extension from config.
        filename = f"{dataset.name}{file_ext}"
        return directory, filename
