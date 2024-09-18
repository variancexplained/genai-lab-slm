#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/repo/profile.py                                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday September 11th 2024 10:14:56 am                                           #
# Modified   : Wednesday September 18th 2024 03:43:09 pm                                           #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Profile Repository Module"""
import logging

import pandas as pd

from discover.application.ops.profile import Profile
from discover.domain.base.repo import Repo
from discover.domain.value_objects.lifecycle import Phase
from discover.infra.database.base import Database


# ------------------------------------------------------------------------------------------------ #
class ProfileRepo(Repo):
    def __init__(self, database: Database) -> None:
        self._database = database
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def __len__(self) -> int:
        return len(self.get_all())

    def add(self, profile: Profile) -> None:
        query = """
            INSERT INTO profile (
                phase,
                stage,
                task,
                start_time,
                end_time,
                runtime_seconds,
                cpu_cores,
                cpu_user_utilization,
                cpu_system_utilization,
                memory_usage_peak_mb,
                memory_allocations,
                file_read_bytes,
                file_write_bytes,
                io_wait_time_seconds,
                network_data_sent_bytes,
                network_data_received_bytes,
                exceptions_raised
            )
            VALUES (
                :phase,
                :stage,
                :task,
                :start_time,
                :end_time,
                :runtime_seconds,
                :cpu_cores,
                :cpu_user_utilization,
                :cpu_system_utilization,
                :memory_usage_peak_mb,
                :memory_allocations,
                :file_read_bytes,
                :file_write_bytes,
                :io_wait_time_seconds,
                :network_data_sent_bytes,
                :network_data_received_bytes,
                :exceptions_raised
            );
            """
        params = profile.as_dict()
        with self._database as db:
            db.command(query=query, params=params)
        self._logger.info(
            f"Inserted profile for phase: {params['phase']} stage: {params['stage']} and task: {params['task']}."
        )

    def get(self, profile_id: int) -> Profile:
        query = """SELECT * FROM profile WHERE id = :id;"""
        params = {"id": profile_id}
        with self._database as db:
            profile = db.query(query=query, params=params)

        return Profile(**profile)

    def get_all(self) -> pd.DataFrame:
        query = """SELECT * FROM profile;"""
        with self._database as db:
            return db.query(query=query)

    def get_by_phase(self, phase: Phase) -> pd.DataFrame:
        query = """SELECT * FROM profile WHERE phase = :phase;"""
        params = {"phase": phase.description}
        with self._database as db:
            return db.query(query=query, params=params)

    def get_by_stage(self, stage: str) -> pd.DataFrame:
        query = """SELECT * FROM profile WHERE stage = :stage;"""
        params = {"stage": stage.description}
        with self._database as db:
            return db.query(query=query, params=params)

    def remove(self, profile_id: int) -> None:
        query = """DELETE FROM profile WHERE id = :id;"""
        params = {"id": profile_id}
        with self._database as db:
            db.command(query=query, params=params)
        self._logger.info(f"Removed profile for profile_id: {profile_id}.")

    def remove_by_phase(self, phase: Phase) -> None:
        query = """DELETE FROM profile WHERE phase = :phase;"""
        params = {"phase": phase.description}
        with self._database as db:
            db.command(query=query, params=params)
        self._logger.info(f"Removed profile data for phase: {phase.description}.")

    def remove_by_stage(self, stage: str) -> None:
        query = """DELETE FROM profile WHERE stage = :stage;"""
        params = {"stage": stage.description}
        with self._database as db:
            db.command(query=query, params=params)
        self._logger.info(f"Removed profile data for stage: {stage.description}.")

    def remove_all(self) -> None:
        confirm = input(
            "Deleting the profile repository is irreversible. Confirm [yes/no] "
        )
        if "yes" in confirm.lower():
            query = """DELETE FROM profile;"""
            with self._database as db:
                db.command(query=query)
            self._logger.info("Removed all profile data")
        else:
            self._logger.info("Removal aborted.")

    def exists(self, profile_id) -> bool:
        query = """SELECT EXISTS(SELECT 1 FROM profile WHERE id = :id);"""
        params = {"id": profile_id}
        with self._database as db:
            result = db.query(query=query, params=params)
            return result.values[0]
