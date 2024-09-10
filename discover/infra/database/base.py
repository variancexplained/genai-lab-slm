#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/database/base.py                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday September 9th 2024 04:44:50 pm                                               #
# Modified   : Monday September 9th 2024 09:48:49 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Module provides basic database interface"""
from __future__ import annotations

import traceback
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

import pandas as pd
import sqlalchemy
from discover.core.data import DataClass
from sqlalchemy.exc import SQLAlchemyError


# ------------------------------------------------------------------------------------------------ #
#                                      DATABASE                                                    #
# ------------------------------------------------------------------------------------------------ #
class Database(ABC):
    """Abstract base class defining the interface for databases."""

    def __enter__(self) -> Database:
        """Enter a transaction block, allowing multiple database stages to be performed as a unit."""
        self.begin()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: traceback.TracebackType,
    ) -> None:
        """Special method takes care of properly releasing the object's resources to the operating system."""
        if exc_type is not None:
            try:
                self.rollback()
            except SQLAlchemyError as e:
                self._logger.exception(
                    f"Exception occurred during rollback.\nException type: {type(e)}\n{e}"
                )
                raise
            self._logger.exception(
                f"Exception occurred.\nException type: {exc_type}\n{exc_value}\n{traceback}"
            )
            raise
        else:
            self.commit()
        self.close()

    @abstractmethod
    def connect(self, autocommit: bool = False) -> Database:
        """Connect to an underlying database.

        Args:
            autocommit (bool): Sets autocommit mode. Default is False.
        """
        pass

    def begin(self) -> None:
        """Begin a transaction block."""
        try:
            if self._connection is None:
                self.connect()
            if self._connection is not None:
                self._transaction = self._connection.begin()
        except AttributeError:
            self.connect()
            if self._connection is not None:
                self._transaction = self._connection.begin()
        except sqlalchemy.exc.InvalidRequestError:
            self.close()
            self.connect()
            if self._connection is not None:
                self._transaction = self._connection.begin()

    def commit(self) -> None:
        """Save pending database stages to the database."""
        try:
            if self._transaction:
                self._transaction.commit()
        except SQLAlchemyError as e:
            self._logger.exception(
                f"Exception occurred during commit.\nException type: {type(e)}\n{e}"
            )
            raise

    def rollback(self) -> None:
        """Restore the database to the state of the last commit."""
        try:
            if self._transaction:
                self._transaction.rollback()
        except SQLAlchemyError as e:
            self._logger.exception(
                f"Exception occurred during rollback.\nException type: {type(e)}\n{e}"
            )
            raise

    def close(self) -> None:
        """Close the database connection."""
        try:
            if self._connection:
                self._connection.close()
                self._connection = None  # Set to None after closing
        except SQLAlchemyError as e:
            self._logger.exception(
                f"Exception occurred during connection close.\nException type: {type(e)}\n{e}"
            )
            raise

    def dispose(self) -> None:
        """Dispose the engine and release resources."""
        try:
            if self._engine:
                self._engine.dispose()
                self._engine = None  # Set to None after disposing
        except SQLAlchemyError as e:
            self._logger.exception(
                f"Exception occurred during engine disposal.\nException type: {type(e)}\n{e}"
            )
            raise

    @abstractmethod
    def insert(
        self,
        data: pd.DataFrame,
        tablename: str,
        dtype: Optional[Dict[str, Any]] = None,
        if_exists: str = "append",
    ) -> Optional[int]:
        """Inserts data in pandas DataFrame format into the designated table.

        Note: This method uses pandas to_sql method. If not in transaction, inserts are
        autocommitted and rollback has no effect. Transaction behavior is extant
        after a begin() or through the use of the context manager.

        Args:
            data (pd.DataFrame): DataFrame containing the data to add to the designated table.
            tablename (str): The name of the table in the database. If the table does not
                exist, it will be created.
            dtype (dict): Dictionary of data types for columns.
            if_exists (str): Action to take if table already exists. Valid values
                are ['append', 'replace', 'fail']. Default = 'append'

        Returns: Number of rows inserted.

        Subclasses should refer to the SQAlchemy documentation for
        supported databases.
        """

    @abstractmethod
    def command(self, query: str, params: Optional[Dict[str, Any]] = None) -> int:
        """Updates or deletes row(s) from the underlying table.

        Args:
            query (str): The SQL command
            params (dict): Parameters for the SQL command

        Returns (int): Number of rows updated/deleted.
        """

    @abstractmethod
    def query(
        self,
        query: str,
        params: dict = {},
        dtypes: Optional[Dict[str, Any]] = None,
        parse_dates: Optional[Dict[str, Any]] = None,
    ) -> pd.DataFrame:
        """Fetches the query result set.
        Args:
            query (str): The SQL command
            params (dict): Parameters for the SQL command
            dtypes (dict): Dictionary mapping of column to data types
            parse_dates (dict): Dictionary of columns and keyword arguments for datetime parsing.

        Returns: Pandas DataFrame

        """


# ------------------------------------------------------------------------------------------------ #
#                               DATABASE ADMINISTRATION                                            #
# ------------------------------------------------------------------------------------------------ #
class DBA(ABC):
    """Abstract base class for database administration classes."""

    @abstractmethod
    def create_table(self, tablename: str) -> None:
        """Creates the designated table.

        Subclasses delegate the creation of specific tables as needed.

        Args:
            tablename (str): Name of table.
        """

    @abstractmethod
    def drop_table(self, tablename: str) -> None:
        """Drops the given table

        Args:
            tablename (str): Name of table to drop
        """

    @abstractmethod
    def backup(self, filepath: str) -> None:
        """Backs up the underlying database.

        Args:
            filepath (str): Filepath for backup. The filename must have a .sql extension.

        """

    @abstractmethod
    def restore(self, filepath: str) -> None:
        """Restore a database from backup

        Args:
            filepath (str): Filepath to backup file.
        """


# ------------------------------------------------------------------------------------------------ #
#                                   DATA ACCESS LAYER                                              #
# ------------------------------------------------------------------------------------------------ #
class DAL(ABC):
    """Abstract base class for Data Access Layers.

    For this project, data are immutable. Hence, update and delete are not supported in the
    data access layers.
    """

    @abstractmethod
    def create(self, data: DataClass):
        """
        Create a new record in the database.

        Args:
            data (Any): The data for the new record.

        Raises:
            NotImplementedError: If the method is not implemented.
        """
        pass

    @abstractmethod
    def read(self, record_id: int) -> Optional[DataClass]:
        """
        Read a single record from the database.

        Args:
            record_id (int): The ID of the record to read.

        Returns:
            Optional[DataClass]: The data of the record, or None if not found.
        """
        pass
