#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/infrastructure/database/setup.py                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday May 28th 2024 03:37:01 pm                                                   #
# Modified   : Monday June 3rd 2024 12:14:59 am                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Database Setup Module"""
from dependency_injector.wiring import Provide, inject

from appinsight.container import AppInsightContainer
from appinsight.infrastructure.database.dba import SQLiteDBA
from appinsight.workflow.task import Task


# ------------------------------------------------------------------------------------------------ #
class CreateDatabasesTask(Task):
    @inject
    def __init__(self, dba: SQLiteDBA = Provide[AppInsightContainer.db.admin]) -> None:
        self._dba = dba()

    def execute(self) -> None:
        try:
            self._dba.create_table(tablename="profile")
        except Exception as e:
            msg = f"Exception occurred while creating dataset. \n{e} "
            self.logger.exception(msg)
            raise
        print("Database has been setup.")
