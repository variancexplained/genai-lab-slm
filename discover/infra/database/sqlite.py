#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/database/sqlite.py                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday September 9th 2024 04:47:41 pm                                               #
# Modified   : Tuesday September 17th 2024 11:28:11 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import logging
import os
from typing import Any, Dict, Optional

import pandas as pd
from sqlalchemy import Connection, RootTransaction, create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from discover.infra.database.base import DBA, Database


# ------------------------------------------------------------------------------------------------ #
class SQLiteDB(Database):
    """Profile Database.

    This class provides methods to interact with a performance database using SQLAlchemy.
    It supports transactions, bulk inserts, command execution, querying, and backup/restore operations.
    """

    def __init__(self, connection_string: str, location: str) -> None:
        """
        Initializes the SQLiteDB instance with a SQLAlchemy engine.

        Args:
            connection_string (str): The database connection string.
            location (str): The location of the database.
        """
        super().__init__()
        os.makedirs(os.path.dirname(location), exist_ok=True)
        self._engine = create_engine(
            connection_string,
            connect_args={
                "check_same_thread": False
            },  # Ensure thread safety for SQLite
        )
        self._connection: Optional[Connection] = None
        self._transaction: Optional[RootTransaction] = None
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def connect(self, autocommit: bool = False) -> Database:
        """
        Returns a connection object for direct access to the database.

        This method can be used by the DBA class or other classes that need to handle
        DDL operations directly or manage raw SQL queries.

        Returns:
            sqlalchemy.engine.Connection: An active connection to the database.

        Raises:
            SQLAlchemyError: If the connection cannot be established.
        """
        try:
            self._connection = self._engine.connect()
            if autocommit:
                self._connection.execution_options(isolation_level="AUTOCOMMIT")
            return self
        except SQLAlchemyError as e:
            self._logger.exception(f"Connection failed: {e}")
            raise

    def insert(
        self,
        data: pd.DataFrame,
        tablename: str,
        dtype: Optional[Dict[str, Any]] = None,
        if_exists: str = "append",
    ) -> Optional[int]:
        """
        Inserts data in pandas DataFrame format into the designated table.

        Args:
            data (pd.DataFrame): DataFrame containing the data to add to the designated table.
            tablename (str): The name of the table in the database. If the table does not exist, it will be created.
            if_exists (str): Action to take if the table already exists. Valid values are ['append', 'replace', 'fail']. Default = 'append'.

        Returns:
            Optional[int]: Number of rows inserted. Can be None if `method='multi'` is used.

        Raises:
            SQLAlchemyError: If the insert operation fails.
        """
        try:
            return data.to_sql(
                tablename,
                con=self._engine,
                if_exists=if_exists,
                index=False,
                dtype=dtype,
                method="multi",
            )
        except SQLAlchemyError as e:
            self._logger.exception(f"Insert failed: {e}")
            raise

    def command(self, query: str, params: Optional[Dict[str, Any]] = None) -> int:
        """
        Executes an SQL command that writes to the database.

        Args:
            query (str): The SQL command.
            params (dict, optional): Parameters for the SQL command.

        Returns:
            int: Number of rows updated/deleted.

        Raises:
            SQLAlchemyError: If the command execution fails.
        """
        try:
            with self._engine.begin() as connection:  # Begin a transaction block
                result = connection.execute(text(query), params)
                return result.rowcount
        except SQLAlchemyError as e:
            self._logger.exception(f"Command execution failed: {e}")
            raise

    def query(
        self,
        query: str,
        params: dict = {},
        dtypes: Optional[Dict[str, Any]] = None,
        parse_dates: Optional[Dict[str, Any]] = None,
    ) -> pd.DataFrame:
        """Executes a query and returns the result set in Pandas DataFrame format.

        Args:
            query (str): The SQL query.
            params (dict, optional): Parameters for the SQL query.

        Returns:
            pd.DataFrame: The query result set.

        Raises:
            SQLAlchemyError: If the query execution fails.
        """
        try:
            return pd.read_sql(query, con=self._engine, params=params)
        except SQLAlchemyError as e:
            self._logger.exception(f"Query execution failed: {e}")
            raise


# ------------------------------------------------------------------------------------------------ #
#                                SQLITE DBA                                                        #
# ------------------------------------------------------------------------------------------------ #


class SQLiteDBA(DBA):
    """SQLITE database administration."""

    def __init__(self, database: SQLiteDB) -> None:
        super().__init__()
        self._database = database
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def create_table(self, schema: str) -> None:
        """Creates a table in the database.

        Args:
            schema (str): SQL schema definition for creating a table.
        """
        with self._database as db:
            db.command(query=schema)

    def exists(self, tablename: str) -> bool:
        """Returns True if the table already exists.

        Args:
            tablename (str): Name of a table.

        Returns:
            bool: True if table exists, False otherwise.
        """
        query = (
            """SELECT name FROM sqlite_master WHERE type = 'table' AND name = :name;"""
        )
        params = {"name": tablename}
        return len(self._database.query(query=query, params=params)) > 0

    def drop_table(self, tablename: str = "profile") -> None:
        """Drops the given table.

        Args:
            tablename (str): Name of table to drop.
        """
        query = f"DROP TABLE IF EXISTS {tablename}"
        self._database.command(query=query)

    def backup(self, backup_file: str) -> None:
        """
        Backs up the database to a specified file.

        Args:
            backup_file (str): The file path to store the backup.

        Raises:
            SQLAlchemyError: If the backup operation fails.
        """
        try:
            with self._database.connect() as connection:
                with open(backup_file, "w") as f:
                    for line in connection.connection.iterdump():
                        f.write(f"{line}\n")
                self._logger.info(f"Database backed up to {backup_file}")
        except (SQLAlchemyError, IOError) as e:
            self._logger.exception(f"Backup failed: {e}")
            raise

    def restore(self, backup_file: str) -> None:
        """
        Restores the database from a specified backup file.

        Args:
            backup_file (str): The file path of the backup to restore.

        Raises:
            SQLAlchemyError: If the restore operation fails.
        """
        try:
            with self._database.connect() as connection:
                with open(backup_file, "r") as f:
                    sql_script = f.read()
                    sql_statements = sql_script.split(";")

                    for sql_statement in sql_statements:
                        if sql_statement.strip():
                            connection.execute(text(sql_statement))

                connection.commit()
                self._logger.info(f"Database restored from {backup_file}")

        except (SQLAlchemyError, FileNotFoundError) as e:
            self._logger.exception(f"Restore failed: {e}")
            raise
