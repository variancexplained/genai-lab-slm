#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appvocai/shared/instrumentation/repo.py                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday May 26th 2024 04:28:43 pm                                                    #
# Modified   : Tuesday August 27th 2024 10:54:14 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Profiling Data Access Layer Module"""
import logging
import os
from typing import Optional

import pandas as pd
from appvocai.shared.config.env import EnvManager
from appvocai.shared.instrumentation.profile import TaskProfile, TaskProfileDTO
from appvocai.shared.persist.database.base import DAL
from appvocai.shared.persist.database.db import SQLiteDB
from appvocai.shared.persist.file.io import IOService
from dotenv import load_dotenv

# ------------------------------------------------------------------------------------------------ #
load_dotenv()


# ------------------------------------------------------------------------------------------------ #
class ProfilingDAL(DAL):
    """
    Profiling Data Access Layer (DAL) class.

    This class provides methods to interact with the profiling database table using a provided Database instance.

    Attributes:
        __TABLENAME (str): The name of the profile database table.
    """

    __TABLENAME = "profile"

    def __init__(self, database: SQLiteDB):
        """
        Initializes the ProfilingDAL instance with a Database instance.

        Args:
            database (Database): The Database instance used for database interactions.
        """
        self._database = database
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def __len__(self) -> int:
        """
        Returns the total number of profiles in the database.

        Returns:
            int: Total number of profiles.
        """
        return len(self.read_all())

    def create(self, profile: TaskProfile) -> int:
        """
        Creates a new profile in the database.

        Args:
            profile (Profile): The profile to create.

        Returns:
            int: The number of rows inserted into the database.
        """
        try:
            data_df = profile.as_df()
            return self._database.insert(data_df, self.__TABLENAME)
        except Exception as e:
            self._logger.exception(f"Error creating data for {print(profile)}.\n{e}")
            raise

    def read(self, id: int) -> Optional[TaskProfileDTO]:
        """
        Reads a single profile from the database based on the profile ID.

        Args:
            id (int): The ID of the profile to read.

        Returns:
            Optional[Profile]: The profile data, or None if the profile with the specified ID does not exist.
        """
        query = "SELECT * FROM profile WHERE id = :id"
        params = {"id": id}
        try:
            result = self._database.query(query, params)

        except Exception as e:
            self._logger.exception(f"Error reading profile for id: {id}.\n{e}")
            raise

        if result.empty:
            return None
        else:
            try:
                return TaskProfileDTO(**result.iloc[0].to_dict())
            except TypeError as te:
                self._logger.exception(
                    f"Error creating Profile from database result. Result:\n{result}\n{te}"
                )
                raise

    def read_by_stage(self, stage: str) -> Optional[pd.DataFrame]:
        """
        Reads multiple profiles from the database based on the stage.

        Args:
            stage (str): The stage to filter the profiles.

        Returns:
            Optional[pd.DataFrame]: A DataFrame containing the profiles matching the specified stage, or None if no profiles found.
        """
        query = "SELECT * FROM profile WHERE stage = :stage"
        params = {"stage": stage}
        try:
            return self._database.query(query, params)
        except Exception as e:
            self._logger.exception(f"Error reading profiles for stage: {stage}.\n{e}")
            raise

    def read_all(self) -> pd.DataFrame:
        """
        Reads all profiles from the database and returns them as a DataFrame.

        Returns:
            pd.DataFrame: DataFrame containing all profiles.
        """
        # Assuming you have a method to execute a query and return a DataFrame
        query = "SELECT * FROM profile"
        return self._database.query(query)

    @classmethod
    def build(cls):
        """Factory method returning an instance of the ProfilingDAL"""
        # Get current configuration file
        em = EnvManager()
        env = em.get_environment()
        try:
            config_file = os.path.join("config", f"{env.lower()}.yml")
        except Exception as e:
            msg = f"{env} is an unrecognized environment setting. \n{e}"
            cls._logger.exception(msg)
            raise

        # Get the current configuration
        try:
            config = IOService.read(config_file)
        except FileNotFoundError as e:
            msg = f"{config_file} is an invalid configuration file. \n{e}"
            cls._logger.exception(msg)
            raise

        # Instantiate Database and DAL
        try:
            db = SQLiteDB(
                connection_string=config["database"]["sqlite"]["url"],
                location=config["database"]["sqlite"]["filepath"],
            )
            # Instantiate and return the DAL
            return cls(database=db)
        except Exception as e:
            msg = f"Exception occurred opening database. \n{e}"
            cls._logger.exception(msg)
            raise
