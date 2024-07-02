#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/infrastructure/persist/database/db.py                                   #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday May 25th 2024 04:11:31 am                                                  #
# Modified   : Sunday June 30th 2024 10:26:09 pm                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import logging
import os

import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError


# ------------------------------------------------------------------------------------------------ #
class SQLiteDB:
    """Profile Database.

    This class provides methods to interact with a performance database using SQLAlchemy.
    It supports transactions, bulk inserts, command execution, querying, and backup/restore operations.
    """

    def __init__(self, connection_string: str, location: str) -> None:
        """
        Initializes the SQLiteDB instance with a SQLAlchemy engine.

        Args:
            connection_string (str): The database connection string.
        """
        os.makedirs(os.path.dirname(location), exist_ok=True)
        self._engine = create_engine(connection_string)
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def insert(
        self, data: pd.DataFrame, tablename: str, if_exists: str = "append"
    ) -> int:
        """
        Inserts data in pandas DataFrame format into the designated table.

        Args:
            data (pd.DataFrame): DataFrame containing the data to add to the designated table.
            tablename (str): The name of the table in the database. If the table does not exist, it will be created.
            if_exists (str): Action to take if table already exists. Valid values are ['append', 'replace', 'fail']. Default = 'append'.

        Returns:
            int: Number of rows inserted.

        Raises:
            SQLAlchemyError: If the insert operation fails.
        """
        try:
            return data.to_sql(
                tablename,
                con=self._engine,
                if_exists=if_exists,
                index=False,
                method="multi",
            )
        except SQLAlchemyError as e:
            self._logger.exception(f"Insert failed: {e}")
            raise

    def command(self, query: str, params: dict = None) -> int:
        """
        Executes an SQL command that modifies the database.

        Args:
            query (str): The SQL command.
            params (dict, optional): Parameters for the SQL command.

        Returns:
            int: Number of rows updated/deleted.

        Raises:
            SQLAlchemyError: If the command execution fails.
        """
        try:
            with self._engine.connect() as connection:
                result = connection.execute(text(query), params)
                connection.commit()
                return result.rowcount
        except SQLAlchemyError as e:
            self._logger.exception(f"Command execution failed: {e}")
            raise

    def create(self, query: str, params: dict = None) -> int:
        """
        Executes an SQL insert that modifies the database.

        Args:
            query (str): The SQL command.
            params (dict, optional): Parameters for the SQL command.

        Returns:
            int: Last row id

        Raises:
            SQLAlchemyError: If the command execution fails.
        """
        try:
            with self._engine.connect() as connection:
                result = connection.execute(text(query), params)
                connection.commit()
                return result.lastrowid
        except SQLAlchemyError as e:
            self._logger.exception(f"Command execution failed: {e}")
            raise

    def query(self, query: str, params: dict = None) -> pd.DataFrame:
        """
        Executes a query and returns the result set in Pandas DataFrame format.

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

    def table_exists(self, tablename: str) -> bool:
        """
        Check if a table exists in a database using SQLAlchemy.

        Args:
            database_url (str): The database URL.
            table_name (str): The name of the table to check for existence.

        Returns:
            bool: True if the table exists, False otherwise.

        Raises:
            sqlalchemy.exc.SQLAlchemyError: If there is an error connecting to the database or checking for the table.
        """
        with self._engine.connect() as connection:
            inspector = inspect(connection)
            return inspector.has_table(table_name=tablename)

    def backup(self, backup_file: str) -> None:
        """
        Backs up the database to a specified file.

        Args:
            backup_file (str): The file path to store the backup.

        Raises:
            SQLAlchemyError: If the backup operation fails.
        """
        try:
            with self._engine.connect() as connection:
                with open(backup_file, "w") as f:
                    for line in connection.connection.iterdump():
                        f.write(f"{line}\n")
                self._logger.info(f"Database backed up to {backup_file}")
        except SQLAlchemyError as e:
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
            with self._engine.connect() as connection:
                with open(backup_file, "r") as f:
                    # Read the SQL script from the backup file
                    sql_script = f.read()

                    # Split the SQL script into individual statements
                    sql_statements = sql_script.split(";")

                    # Execute each SQL statement
                    for sql_statement in sql_statements:
                        # Skip empty statements
                        if sql_statement.strip():
                            # Execute the SQL statement
                            connection.execute(text(sql_statement))

                # Commit the transaction
                connection.commit()
                self._logger.info(f"Database restored from {backup_file}")

        except SQLAlchemyError as e:
            self._logger.exception(f"Restore failed: {e}")
            raise
