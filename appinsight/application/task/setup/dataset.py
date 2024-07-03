#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/application/task/setup/dataset.py                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday July 3rd 2024 08:20:30 am                                                 #
# Modified   : Wednesday July 3rd 2024 03:11:59 pm                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Database Setup Module"""
from dependency_injector.wiring import Provide, inject

from appinsight.application.base import Task
from appinsight.infrastructure.dependency.container import AppInsightContainer
from appinsight.infrastructure.persist.database.base import Database


# ------------------------------------------------------------------------------------------------ #
class DatasetDBSetupTask(Task):
    """Executes the DDL to setup a dataset table.

    The DatasetDBSetupTask gets a configuration object for the table, containing the sql for
    three subtasks:
        exists: Evaluate existence of a table in the database
        drop: Drops the existing table, if it exists.
        create: Creates the table.

    The table is created if it doesn't exist, or the force parameter is True. Before the
    table is created, any existing table is backed up.

    Args:
        config (dict): Dictionary containing the filepaths to the exists, drop
            and create DDL statemeents.
        db (Database): A database object to process the DDL statements.


    This class encapsulates the creation of the dataset table.
    """

    @inject
    def __init__(
        self,
        sql_filepath: str,
        sql_drop_filepath: str,
        sql_create_filepath: str,
        db: Database = Provide[AppInsightContainer.db.sqlite],
        force: bool = False,
    ) -> None:
        self._sql_filepath = sql_filepath
        self._db = db()

    def execute_task(self) -> None:
        """Executes the process of creating the dataset table."""

        # Drop
        with open(self._sql_filepath) as file:
            query = file.read()
            self._db.command(query=query)
