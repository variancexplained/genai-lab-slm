#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/storage/repo/__pycache__/repo.py                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday September 9th 2024 07:43:56 pm                                               #
# Modified   : Saturday September 14th 2024 06:48:26 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Profile Repository Module"""
import logging

import pandas as pd

from discover.domain.base.repo import Repo
from discover.infra.database.base import Database
from discover.infra.monitor.profile import Profile


# ------------------------------------------------------------------------------------------------ #
class ProfileRepo(Repo):
    """
    Profile repository class responsible for performing CRUD operations on the 'profile' table.
    It provides methods for adding, retrieving, and removing profile records from the database,
    as well as checking for the existence of a profile by its ID.

    Attributes:
        _database (Database): The database instance used for executing SQL queries and commands.
        _logger (logging.Logger): Logger instance for logging operations within the repository.

    Methods:
        __len__(): Returns the total number of profiles in the database.
        add(profile: Profile): Adds a new profile to the 'profile' table.
        get(profile_id: int): Retrieves a profile by its ID.
        get_all(): Retrieves all profiles as a pandas DataFrame.
        get_by_task(task_name: str): Retrieves profiles that match a specific task name.
        get_by_stage(stage: str): Retrieves profiles that match a specific stage.
        remove(profile_id: int): Removes a profile by its ID.
        remove_by_task(task_name: str): Removes profiles that match a specific task name.
        remove_by_stage(stage: str): Removes profiles that match a specific stage.
        exists(profile_id: int): Checks if a profile exists by its ID.
    """

    def __init__(self, database: Database) -> None:
        """
        Initializes the ProfileRepo with the given database.

        Args:
            database (Database): An instance of the database used for executing SQL commands and queries.
        """
        self._database = database
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def __len__(self) -> int:
        """
        Returns the total number of profiles in the database.

        Returns:
            int: The total number of profiles in the 'profile' table.
        """
        return len(self.get_all())

    def add(self, profile: Profile) -> None:
        """
        Adds a new profile to the 'profile' table in the database.

        Args:
            profile (Profile): The profile object to be added to the database.
        """
        query = """
            INSERT INTO profile (
                env, stage, task_name, task_start_time, task_end_time, runtime_seconds,
                cpu_cores, cpu_user_utilization, cpu_system_utilization, memory_usage_peak_mb,
                memory_allocations, file_read_bytes, file_write_bytes, io_wait_time_seconds,
                network_data_sent_bytes, network_data_received_bytes, exceptions_raised, retry_count
            )
            VALUES (
                :env, :stage, :task_name, :task_start_time, :task_end_time, :runtime_seconds,
                :cpu_cores, :cpu_user_utilization, :cpu_system_utilization, :memory_usage_peak_mb,
                :memory_allocations, :file_read_bytes, :file_write_bytes, :io_wait_time_seconds,
                :network_data_sent_bytes, :network_data_received_bytes, :exceptions_raised, :retry_count
            );
        """
        params = profile.as_dict()
        with self._database as db:
            db.command(query=query, params=params)

    def get(self, profile_id: int) -> Profile:
        """
        Retrieves a profile from the database by its ID.

        Args:
            profile_id (int): The ID of the profile to retrieve.

        Returns:
            Profile: The profile object retrieved from the database.
        """
        query = """SELECT * FROM profile WHERE id = :id;"""
        params = {"id": profile_id}
        with self._database as db:
            profile = db.query(query=query, params=params)

        return Profile(**profile)

    def get_all(self) -> pd.DataFrame:
        """
        Retrieves all profiles from the database as a pandas DataFrame.

        Returns:
            pd.DataFrame: A DataFrame containing all profile records.
        """
        query = """SELECT * FROM profile;"""
        with self._database as db:
            return db.query(query=query)

    def get_by_task(self, task_name: str) -> pd.DataFrame:
        """
        Retrieves profiles from the database that match a specific task name.

        Args:
            task_name (str): The task name to filter profiles by.

        Returns:
            pd.DataFrame: A DataFrame containing profiles that match the task name.
        """
        query = """SELECT * FROM profile WHERE task_name = :task_name;"""
        params = {"task_name": task_name}
        with self._database as db:
            return db.query(query=query, params=params)

    def get_by_stage(self, stage: str) -> pd.DataFrame:
        """
        Retrieves profiles from the database that match a specific stage.

        Args:
            stage (str): The stage to filter profiles by.

        Returns:
            pd.DataFrame: A DataFrame containing profiles that match the stage.
        """
        query = """SELECT * FROM profile WHERE stage = :stage;"""
        params = {"stage": stage}
        with self._database as db:
            return db.query(query=query, params=params)

    def remove(self, profile_id: int) -> None:
        """
        Removes a profile from the database by its ID.

        Args:
            profile_id (int): The ID of the profile to remove.
        """
        query = """DELETE FROM profile WHERE id = :id;"""
        params = {"id": profile_id}
        with self._database as db:
            db.command(query=query, params=params)

    def remove_by_task(self, task_name: str) -> None:
        """
        Removes profiles from the database that match a specific task name.

        Args:
            task_name (str): The task name to filter profiles to be removed.
        """
        query = """DELETE FROM profile WHERE task_name = :task_name;"""
        params = {"task_name": task_name}
        with self._database as db:
            db.command(query=query, params=params)

    def remove_by_stage(self, stage: str) -> None:
        """
        Removes profiles from the database that match a specific stage.

        Args:
            stage (str): The stage to filter profiles to be removed.
        """
        query = """DELETE FROM profile WHERE stage = :stage;"""
        params = {"stage": stage}
        with self._database as db:
            db.command(query=query, params=params)

    def exists(self, profile_id: int) -> bool:
        """
        Checks if a profile exists in the database by its ID.

        Args:
            profile_id (int): The ID of the profile to check.

        Returns:
            bool: True if the profile exists, False otherwise.
        """
        query = """SELECT EXISTS(SELECT 1 FROM profile WHERE id = :id);"""
        params = {"id": profile_id}
        with self._database as db:
            result = db.query(query=query, params=params)
            return result.values[0]
