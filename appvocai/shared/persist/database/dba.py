#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appvocai/shared/persist/database/dba.py                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday May 25th 2024 08:19:26 am                                                  #
# Modified   : Tuesday August 27th 2024 10:54:14 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Database Administration Module"""
from appvocai.shared.persist.database.base import DBA, Database

# ------------------------------------------------------------------------------------------------ #


class SQLiteDBA(DBA):
    """SQLITE database administration."""

    def __init__(self, database: Database) -> None:
        super().__init__()
        self._database = database

    def create_table(self, tablename: str = "profile") -> None:
        """Creates the table on the database

        Args:
            tablename (str): Name of table
        """
        if tablename == "profile":
            self._create_profile_table()
        else:
            raise ValueError(f"Table {tablename} does not exist.")

    def exists(self, tablename: str) -> bool:
        """Returns True if the table already exists

        Args:
            tablename (str): Name of a table.

        Returns:
            Bool: True if table exists False otherwise.

        """
        return self._database.table_exists(tablename=tablename)

    def drop_table(self, tablename: str = "profile") -> None:
        """Drops the given table

        Args:
            tablename (str): Name of table to drop
        """
        if tablename == "profile":
            self._drop_profile_table()
        else:
            raise ValueError(f"Table {tablename} does not exist.")

    def table_exists(self, tablename: str) -> bool:
        """Check if table exists

        Args:
            tablename (str): Name of table.
        """
        return self._database.table_exists(tablename)

    def backup(self, filepath: str) -> None:
        """Backs up the underlying database.

        Args:
            filepath (str): Filepath for backup. The filename must have a .sql extension.

        """
        self._database.backup(backup_file=filepath)

    def restore(self, filepath: str) -> None:
        """Restore a database from backup

        Args:
            filepath (str): Filepath to backup file.
        """
        self._database.restore(backup_file=filepath)

    def _create_profile_table(self) -> None:
        """Creates the table on the database

        Args:
            tablename (str): Name of table
        """
        sql = """CREATE TABLE IF NOT EXISTS profile (
         id INTEGER NOT NULL PRIMARY KEY,
         task TEXT NOT NULL,
         stage TEXT NOT NULL,
         phase TEXT NOT NULL,
         runtime FLOAT NOT NULL,
         cpu_usage_pct FLOAT NOT NULL,
         memory_usage FLOAT NOT NULL,
         disk_read_bytes INTEGER NOT NULL,
         disk_write_bytes INTEGER NOT NULL,
         disk_total_bytes INTEGER NOT NULL,
         input_records INTEGER NOT NULL,
         output_records INTEGER NOT NULL,
         input_records_per_second FLOAT NOT NULL,
         output_records_per_second FLOAT NOT NULL,
         timestamp TIMESTAMP NOT NULL);
        """
        self._database.command(query=sql)

    def _drop_profile_table(self) -> None:
        sql = """DROP TABLE IF EXISTS profile"""
        self._database.command(query=sql)
