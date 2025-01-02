#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/persist/object/flowstate.py                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday January 1st 2025 02:49:35 am                                              #
# Modified   : Thursday January 2nd 2025 09:38:52 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""FlowState Database Module"""
import logging
import shelve
from datetime import datetime
from typing import Dict

from discover.asset.dataset.passport import DatasetPassport
from discover.core.flow import FlowStateDef, PhaseDef, StageDef
from discover.infra.exception.object import (
    ObjectDatabaseNotFoundError,
    ObjectIOException,
    ObjectNotFoundError,
)
from discover.infra.persist.object.base import DAO

# ------------------------------------------------------------------------------------------------ #


class FlowState(DAO):
    """
    Manages the flow state of a pipeline, providing persistent storage and retrieval
    of dataset passports for different phases and stages.

    This class uses a `shelve` database to persistently store dataset passports,
    allowing stages of a pipeline to share and update state information across runs.
    """

    def __init__(self, db_path: str) -> None:
        """
        Initializes the FlowState instance with the specified database path.

        Args:
            db_path (str): Path to the shelve database file for storing flow states.
        """
        self._db_path = db_path
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @property
    def count(self) -> int:
        """
        Returns the total number of dataset passports stored in the database.

        Returns:
            int: The count of dataset passports in the database.
        """
        return len(self.read_all)

    def create(self, passport: DatasetPassport) -> None:
        """
        Stores a dataset passport in the database, marking it as PENDING.

        Args:
            passport (DatasetPassport): The dataset passport to be stored.

        Raises:
            ObjectDatabaseNotFoundError: If the database file cannot be located.
            ObjectIOException: If any other error occurs while writing to the database.
        """
        key = self._format_key(
            phase=passport.phase, stage=passport.stage, state=passport.state
        )
        passport.state = FlowStateDef.PENDING
        try:
            with shelve.open(self._db_path) as db:
                db[key] = passport
        except FileNotFoundError as e:
            msg = (
                f"The flow state object database was not found at {self._db_path}.\n{e}"
            )
            self._logger.exception(msg)
            raise ObjectDatabaseNotFoundError(msg)
        except Exception as e:
            msg = f"Unknown exception occurred while creating the flow state database.\n{e}"
            self._logger.exception(msg)
            raise ObjectIOException(msg, e) from e

    def read(
        self,
        phase: PhaseDef,
        stage: StageDef,
        state: FlowStateDef = FlowStateDef.PENDING,
    ) -> DatasetPassport:
        """
        Retrieves a dataset passport for a specific phase, stage, and state,
        and updates its status to indicate processing has started.

        Args:
            phase (PhaseDef): The phase identifier.
            stage (StageDef): The stage identifier.
            state (FlowStateDef, optional): The current state of the passport. Defaults to PENDING.

        Returns:
            DatasetPassport: The retrieved and updated dataset passport.

        Raises:
            ObjectNotFoundError: If no passport exists for the specified key.
            ObjectDatabaseNotFoundError: If the database file cannot be located.
            ObjectIOException: If any other error occurs while accessing the database.
        """
        key = self._format_key(phase=phase, stage=stage, state=state)
        try:
            with shelve.open(self._db_path) as db:
                passport = db[key]
                passport.started = datetime.now()
                passport.state = FlowStateDef.STARTED
                db[key] = passport
            return passport
        except KeyError:
            msg = f"No passports exist for {key}."
            self._logger.error(msg)
            raise ObjectNotFoundError(msg)
        except FileNotFoundError as e:
            msg = (
                f"The flow state object database was not found at {self._db_path}.\n{e}"
            )
            self._logger.exception(msg)
            raise ObjectDatabaseNotFoundError(msg, e) from e
        except Exception as e:
            msg = f"Unknown exception occurred while reading the flow state database.\n{e}"
            self._logger.exception(msg)
            raise ObjectIOException(msg, e) from e

    def read_all(self) -> Dict[str, DatasetPassport]:
        """
        Retrieves all dataset passports stored in the database.

        Returns:
            Dict[str, DatasetPassport]: A dictionary of all stored passports,
            keyed by their phase, stage, and state.

        Raises:
            ObjectDatabaseNotFoundError: If the database file cannot be located.
            ObjectIOException: If any other error occurs while reading from the database.
        """
        try:
            with shelve.open(self._db_path, flag="r") as db:
                return dict(db.items())
        except FileNotFoundError as e:
            msg = f"The object database for flow state was not found at {self._db_path}.\n{e}"
            self._logger.exception(msg)
            raise ObjectDatabaseNotFoundError(msg, e) from e
        except Exception as e:
            msg = f"Unknown exception occurred while reading from the flow state database.\n{e}"
            self._logger.exception(msg)
            raise ObjectIOException(msg, e) from e

    def _format_key(self, phase: PhaseDef, stage: StageDef, state: FlowStateDef) -> str:
        """
        Formats the key for accessing a dataset passport in the database.

        Args:
            phase (PhaseDef): The phase identifier.
            stage (StageDef): The stage identifier.
            state (FlowStateDef): The state of the passport.

        Returns:
            str: A formatted key string representing the combination of phase, stage, and state.
        """
        return f"{phase.value}_{stage.value}_{state.value}"
