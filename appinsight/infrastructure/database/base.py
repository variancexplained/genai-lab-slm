#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/infrastructure/database/base.py                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday May 25th 2024 03:16:08 am                                                  #
# Modified   : Friday May 31st 2024 02:53:38 am                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Module provides basic database interface"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

import pandas as pd

from appinsight.utils.data import DataClass


# ------------------------------------------------------------------------------------------------ #
#                                      DATABASE                                                    #
# ------------------------------------------------------------------------------------------------ #
class Database(ABC):
    """Abstract base class defining the interface for databases."""

    @abstractmethod
    def insert(
        self,
        data: pd.DataFrame,
        tablename: str,
        dtype: dict = None,
        if_exists: str = "append",
    ) -> int:
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
    def command(self, query: str, params: dict = None) -> int:
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
        params: dict = (),
        dtypes: dict = None,
        parse_dates: dict = None,
    ) -> pd.DataFrame:
        """Fetches the query result set.
        Args:
            query (str): The SQL command
            params (dict): Parameters for the SQL command
            dtypes (dict): Dictionary mapping of column to data types
            parse_dates (dict): Dictionary of columns and keyword arguments for datetime parsing.

        Returns: Pandas DataFrame

        """

    @abstractmethod
    def _execute(self, query: str, params: dict = ()) -> list:
        """Execute method reserved primarily for updates, and deletes, as opposed to queries returning data.

        This method fullfills the command method.

        Args:
            query (str): The SQL command
            params (dict): Parameters for the SQL command

        Returns (int): Number of rows updated or deleted.

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
